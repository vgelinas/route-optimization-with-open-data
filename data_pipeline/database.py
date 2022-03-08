"""
Database wrapper class. Automatically handles queries as batched queries.  
"""
import pandas as pd
import numpy as np 
import db_tables 
import db_connection 
import sqlalchemy
from sqlalchemy.inspection import inspect 


class DatabaseWrapper:

    def __init__(self, session=None):
        self.session = session 
        self.db_tables = {
            "agencies": db_tables.Agencies,
            "routes": db_tables.Routes,
            "directions": db_tables.Directions,
            "stops": db_tables.Stops, 
            "schedules": db_tables.Schedules,
            "vehicles": db_tables.Vehicles,
            "vehicles_validation": db_tables.VehiclesValidation,
            "vehicle_locations": db_tables.VehicleLocations, 
            "vehicle_locations_validation": db_tables.VehicleLocationsValidation, 
            "connections": db_tables.Connections,
            "transit_graph": db_tables.TransitGraph 
        }

    def connect(self):
        """Get a database connection.

        Returns:
            conn: SQLAlchemy connection. 
        """
        return db_connection.create_engine().connect() 

    def _check_if_primary_key_in_table(self, tablename, key):
        """Helper function for insert_dataframe_in_table method.
        This is used to check for known keys before inserting duplicates.

        Args:
            tablename (str): Name of ORM table.
            key: Primary key for that table, of the intended type. 

        Returns:
            bool: Whether key is in database table. 
        """

        table = self.db_tables[tablename]  # ORM table  
        primary_key = inspect(table).primary_key[0].name   

        result = self.session.query(getattr(table, primary_key)).filter(
                        getattr(table, primary_key)==key)

        exists_bool = self.session.query(result.exists()).scalar() 
        return exists_bool 

    def start_session(self):
        self.session = db_connection.create_session()

    def insert_dataframe_in_table(self, tablename, dataframe):
        """Insert dataframe in database, updating existing primary keys. 
        Assumes the dataframe format matches the type of the table. 

        Args:
            tablename (str): Name of database table, e.g. 'routes'.
            dataframe (dataframe): Table of values to be inserted. 
        """

        if dataframe is None:
            return 

        if self.session is None:
            self.start_session() 

        df = dataframe.replace([np.nan], [None]) 
        table = self.db_tables[tablename]  # ORM table  

        # Split primary keys according to which already exist in table.
        # We bulk update the existing keys, and bulk insert the new ones.
        primary_key = inspect(table).primary_key[0].name   

        known_keys = filter(
                        lambda k: self._check_if_primary_key_in_table(tablename, k),
                        df[primary_key].values 
                        )
        known_keys = list(known_keys) 

        df_known_keys = df[df[primary_key].isin(known_keys)]  
        df_unknown_keys = df[~df[primary_key].isin(known_keys)]  
         
        self.session.bulk_update_mappings(
            table, df_known_keys.to_dict("records"))  

        self.session.bulk_insert_mappings(
            table, df_unknown_keys.to_dict("records")) 

        self.session.commit() 

    def update_dataframe_in_table(self, tablename, dataframe):
        """Update dataframe in database.  

        Args:
            tablename (str): Name of database table, e.g. 'routes'.
            dataframe (dataframe): Table of values to be updated. 
        """

        if dataframe is None:
            return 

        if self.session is None:
            self.start_session() 

        df = dataframe.replace([np.nan], [None])  # input validation 
        table = self.db_tables[tablename]         # ORM table  

        self.session.bulk_update_mappings(table, df.to_dict("records"))  
        self.session.commit() 

    def query(self, query, chunksize=1000):
        """Fetch result of a SELECT query from database. Uses batch querying.
        Essentially a wrapper for pd.read_sql(). 

        Args:
            query (str): SQL SELECT query to read. 
            chunksize (int, optional): Size used for batch querying. Defaults to 1000.

        Returns:
            dataframe: Result of the SELECT query. 
        """

        with self.connect().execution_options(stream_results=True) as conn:
            df_list = [] 
            for chunk_dataframe in pd.read_sql(query, conn, chunksize=chunksize): 
                df_list.append(chunk_dataframe) 

        return pd.concat(df_list)

    def get_agency_tag(self):
        """Get the agency tag from database (e.g. 'ttc'). This is mainly used
        for querying the NextBus API which needs this in every argument. 

        This assumes there's a single agency in use, and otherwise returns 
        the first tag.

        Returns:
            str: Agency tag.
        """

        df = self.query("SELECT tag FROM agencies")  
        return df.tag.values[0] 

    def get_route_list(self, agency_tag):
        """Get a list of distinct route tags from the database. 
        Tags are naturally strings, but the list will be ordered as ints
        when coercion is possible.  

        Returns:
            List[str]: List containing all route tags. 
        """

        df = self.query(
            f"SELECT DISTINCT tag FROM routes where agency_tag='{agency_tag}'")
        route_list = list(df.tag.unique()) 

        # Tags are str by default, but may actually be integers depending on agency.
        # In this case, sort as integers; this makes it nicer when fetching data
        # from the web API on verbose mode.  
        try:
            route_list.sort(key = lambda x: int(x)) 
        except:
            pass

        return route_list 

    def get_direction_list(self, agency_tag): 
        """Get a list of distinct direction tags from the database. 

        Returns:
            List[str]: List of all direction tags.  
        """

        df = self.query(
            f"SELECT DISTINCT tag FROM directions WHERE agency_tag='{agency_tag}'") 
        direction_list = list(df.tag.unique()) 

        return direction_list 

    def get_stop_coords_dataframe(self, agency_tag):
        """Get all stop tags from the stops table with their accompanying
        (lat, lon) coordinates. Stops are sorted by lat, then lon values.

        Returns:
            dataframe: Stops dataframe with tag, lat, lon columns. 
        """

        df = self.query(
            f""" 
             SELECT DISTINCT tag, lat, lon
             FROM stops
             WHERE agency_tag='{agency_tag}'
             ORDER BY 2,3
            """
            )

        return df 

    def get_stops_along_direction_dataframe(self, direction_tag, agency_tag):
        """Get all stop tags along a direction, along with stop numbers. 

        Returns:
            dataframe: Stops dataframe with stop_tag, direction_tag,
                       and stop_along_direction column. 
        """
        request = f""" 
                    SELECT routes.title as route,
                           routes.tag as route_tag,
                           directions.title as direction,
                           directions.tag as direction_tag,
                           directions.name as heading,
                           stops.title as stop,
                           stops.tag as stop_tag,
                           stops.lat as stop_lat,
                           stops.lon as stop_long,
                           stops.stop_along_direction as stop_number
               
                      FROM routes
                      INNER JOIN directions ON directions.route_tag = routes.tag
                      INNER JOIN stops      ON stops.direction_tag = directions.tag
            
                     WHERE routes.agency_tag='{agency_tag}'
                       AND directions.tag='{direction_tag}'
                     ORDER BY stop_number
                   """
        return self.query(request)  

    def get_connections_dataframe(self):
        """Fetch the entire connections table.

        Returns:
            dataframe: The connections table as a dataframe.   
        """
        return self.query("SELECT * FROM connections") 

    def get_known_vehicle_ids(self, agency_tag):
        """Fetch list of all known vehicle ids for the agency."""

        df_vehicles = self.query(
            f"SELECT DISTINCT id FROM vehicles WHERE agency_tag='{agency_tag}'")
        return list(df_vehicles.id.unique()) 

    def get_active_vehicle_ids(self, agency_tag, num_days=7): 
        """Fetch list of vehicle ids which have been active on a route
        over the last num_days.  
        """
        prior_days = num_days - 1

        df_vehicles = self.query(
            f"""SELECT DISTINCT id   
                  FROM vehicles 
                 WHERE agency_tag='{agency_tag}'
                 AND last_seen_active >= SUBDATE(CURRENT_DATE(), INTERVAL {prior_days} DAY)
            """)  
        return df_vehicles.id.to_list() 

    def get_validation_vehicle_ids(self, agency_tag):
        """Fetch all vehicle ids from the vehicles_validation table."""

        df_vehicles = self.query("SELECT DISTINCT id FROM vehicles_validation")
        return df_vehicles.id.to_list() 