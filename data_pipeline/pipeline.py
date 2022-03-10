"""
Data Pipeline classes. 
"""
import contextlib
import datetime
import db_connection 
import pandas as pd
from database import DatabaseWrapper
from nextbus_api import NextBusAPI 
from sklearn.neighbors import KNeighborsRegressor
from utils.batching import batches
from utils.configs import get_transit_config
from utils.distances import calculate_distance_from_lat_lon_coords
from utils.queries import get_queries_path


class Pipeline:

    def __init__(self, verbose=False):  
        self.verbose = verbose
        self.session = db_connection.create_session() 
        self.db = DatabaseWrapper(session=self.session) 
        self.data_loader = DataLoader(
                            db=self.db, 
                            session=self.session, 
                            verbose=self.verbose)  
        self.data_preparation = DataPreparation(
                            db=self.db, 
                            session=self.session)  


class DataLoader:

    def __init__(self, db, session, wait_time=0, verbose=False):
        self.verbose = verbose 
        self.db = db 
        self.session = session 
        self._wait_time = wait_time
        self.nextbus = NextBusAPI(verbose=self.verbose, wait_time=self._wait_time)  
        self.parser = ResponseParser()

    def set_wait_time(self, wait_time):
        self._wait_time = wait_time
        self.nextbus = NextBusAPI(verbose=self.verbose, wait_time=self._wait_time)  

    def set_verbose(self, verbose):
        self.verbose = verbose
        self.nextbus = NextBusAPI(verbose=self.verbose, wait_time=self._wait_time)  

    def populate_transit_config_tables_from_API(self):
        """Download all routes, directions and stops data and insert them
        into the database. 
        """
        # First, collect list of routes for agency. 
        agency_tag = self.db.get_agency_tag() 
        route_list_response = self.nextbus.get_response_dict_from_web(
                                            endpoint_name="routeList",
                                            agency_tag=agency_tag
                                            )

        routes_df_dict = self.parser.parse_route_list_response_into_df_dict(
                                            response_dict=route_list_response,
                                            agency_tag=agency_tag
                                            )
        
        # Insert the list of route tags in the routes table.
        # This response dataframe only contains partial columns, and
        # the other columns will be updated as we collect them from
        # the routeConfig endpoint.
        self.db.insert_dataframe_in_table("routes", routes_df_dict["routes"])

        # Next we collect the route config info.
        # We'll update the remaining 'routes' table 
        # columns, and populate the entire 'directions' & 'stops' tables. 
        route_list = routes_df_dict["routes"].tag.unique()    
        for route_tag in route_list:   

            route_config_response = self.nextbus.get_response_dict_from_web(
                                            endpoint_name="routeConfig",
                                            agency_tag=agency_tag,
                                            route_tag=route_tag
                                            ) 

            config_df_dict = self.parser.parse_route_config_response_into_df_dict(
                                            response_dict=route_config_response,
                                            route_tag=route_tag,
                                            agency_tag=agency_tag
                                            )

            self.db.update_dataframe_in_table("routes", config_df_dict["routes"])
            self.db.insert_dataframe_in_table("directions", config_df_dict["directions"])
            self.db.insert_dataframe_in_table("stops", config_df_dict["stops"]) 

    def populate_schedules_table_from_API(self):
        """Download all schedules tables and insert them into database."""
        # Get API args. 
        agency_tag = self.db.get_agency_tag() 
        route_list = self.db.get_route_list(agency_tag) 

        for route_tag in route_list: 

            time_of_extraction = datetime.datetime.now()
            schedules_response = self.nextbus.get_response_dict_from_web(
                                                endpoint_name="schedule",
                                                route_tag=route_tag,
                                                agency_tag=agency_tag
                                                )

            df_dict = self.parser.parse_schedule_response_into_df_dict(
                                                response_dict=schedules_response,
                                                route_tag=route_tag,
                                                agency_tag=agency_tag,
                                                time_of_extraction=time_of_extraction
                                                )

            self.db.insert_dataframe_in_table("schedules", df_dict["schedules"])

    def fetch_active_vehicles_snapshop_from_API(self):
        """Fetch the id of all currently active vehicles and insert in db."""

        agency_tag = self.db.get_agency_tag() 
        route_list = self.db.get_route_list(agency_tag)   

        df_list = []
        for route_tag in route_list:
            df_vehicles_on_route = self._fetch_vehicle_location_on_route_df(
                                            route_tag, agency_tag)
            df_list.append(df_vehicles_on_route)
        df_active_vehicles = pd.concat(df_list)  

        df_active_vehicles["agency_tag"] = agency_tag
        df_active_vehicles = df_active_vehicles[["id", "read_time", "agency_tag"]] 
        df_active_vehicles.rename(columns={"read_time": "last_seen_active"},
                                  inplace=True)

        self.db.insert_dataframe_in_table("vehicles", df_active_vehicles)        

    def _fetch_vehicle_location_on_route_df(self, route_tag, agency_tag):
        """Fetch vehicle data for all vehicles currently active on route."""
        
        time_of_extraction = datetime.datetime.now() 
        response_dict = self.nextbus.get_response_dict_from_web(
                                endpoint_name="vehicleLocations",
                                agency_tag=agency_tag,
                                route_tag=route_tag,
                                epoch_time_in_msec=0 
                                )

        df_dict = self.parser.parse_vehicle_locations_response_into_df_dict(
                                response_dict=response_dict,
                                agency_tag=agency_tag,
                                time_of_extraction=time_of_extraction
                                )

        return df_dict["vehicle_locations"]

    def fetch_vehicle_locations_from_API(self, active_over_num_days=7):
        """Fetch current vehicle location for all recently active vehicle ids."""  

        agency_tag = self.db.get_agency_tag() 
        vehicle_ids = self.db.get_active_vehicle_ids(agency_tag, active_over_num_days)

        df_list = [] 
        for vehicle_id in vehicle_ids:
            df_vehicle = self._fetch_vehicle_location_df(agency_tag, vehicle_id) 
            df_list.append(df_vehicle)
        df_vehicle_locations = pd.concat(df_list) 

        self.db.insert_dataframe_in_table("vehicle_locations", df_vehicle_locations)

    def _fetch_vehicle_location_df(self, agency_tag, vehicle_id):
        """Fetch current location data for a specific vehicle.""" 

        time_of_extraction = datetime.datetime.now()
        response_dict = self.nextbus.get_response_dict_from_web(
                                endpoint_name="vehicleLocation",
                                agency_tag=agency_tag,
                                vehicle_id=vehicle_id 
                                )

        df_dict = self.parser.parse_vehicle_locations_response_into_df_dict(
                                response_dict=response_dict,
                                agency_tag=agency_tag,
                                time_of_extraction=time_of_extraction
                                )

        return df_dict["vehicle_locations"]

    def fetch_validation_vehicle_locations_from_API(self):
        """Fetch location data for vehicles from the vehicles_validation table,
        then insert into the vehicle_locations_validation table.
        
        This method is intended to collect very precise data on a select
        few vehicles to create and maintain validation datasets. 
        """
        agency_tag = self.db.get_agency_tag() 
        vehicle_ids = self.db.get_validation_vehicle_ids(agency_tag) 

        df_list = [] 
        for vehicle_id in vehicle_ids:
            df_vehicle = self._fetch_vehicle_location_df(agency_tag, vehicle_id) 
            df_list.append(df_vehicle)

        df_vehicle_locations = pd.concat(df_list) 
        self.db.insert_dataframe_in_table("vehicle_locations_validation", df_vehicle_locations)

        pass

    def delete_old_vehicle_locations_entries(self, keep_num_days=7):
        """Delete all vehicle location entries outside of retention period."""

        today = datetime.datetime.today().replace(
                        hour=0, minute=0, second=0, microsecond=0)

        days_kept_before_today = keep_num_days - 1
        first_date_kept = (today - datetime.timedelta(days=days_kept_before_today)).strftime("%Y-%m-%d")

        self.db.session.execute(f"DELETE FROM vehicle_locations WHERE read_time < '{first_date_kept}'")
        self.db.session.commit() 


class DataPreparation:

    def __init__(self, db, session):
        self.db = db 
        self.session = session

    def populate_connections_table(self):
        """Cluster nearby stops within a fixed distance. This distance can be
        adjusted within the transit config file. 

        Stop pairs are inserted in the database (in both directions) along with
        their latitude, longitude coordinates and the direction they're on. 
        """

        # First fetch the cluster max distance from a flat file. 
        cluster_distance = None   
        config = get_transit_config() 
        cluster_distance = config["connections_cluster_max_distance_meters"]     

        # Then build and insert connections table. 
        df_connections = self._build_connections_df_from_database(
                                        cluster_distance=cluster_distance) 

        self.db.insert_dataframe_in_table("connections", df_connections)  

    def _build_connections_df_from_database(self, cluster_distance):
        """Assemble the connections dataframe from the stops table.
        Helper function for population_connections_table. 

        The dataframe has the following column format:
           - key (str),
           - stop1 (str), 
           - lat1 (str), 
           - lon1 (float), 
           - stop2 (str),  
           - lat2 (float),
           - lon2 (float), 
           - distance_meters (float) 

        Args:
            cluster_distance (float): Maximal meter distance between pairs.

        Returns:
            df: Dataframe to be inserted in 'connections' table.
        """

        connections_types = {
            "key": "str",
            "stop1": "str",
            "lat1": "float", 
            "lon1": "float",
            "stop2": "str", 
            "lat2": "float",
            "lon2": "float", 
            "distance_meters": "float" 
        }
        agency_tag = self.db.get_agency_tag() 
        stops_df = self.db.get_stop_coords_dataframe(agency_tag=agency_tag) 

        # Algorithm: Neighborhood Search 
        # 0. First, stops_df already comes sorted by lat, then lon values.
        # 1. Find a threshold value so that latitude differences above that 
        #    threshold are guaranteed to be > cluster_distance away. Do 
        #    the same for longitude.   
        # 2. For stop in list: 
        #       - find all stops within [lat += lat_thresh, lon += lon_thresh]
        #       - test whether within cluster distance
        #       - add to list of pairs 
        df_pairs_list = [] 
        stops = stops_df.tag.unique() 

        # 1. Find lat_threshold and lon_threshold to form neighborhoods. 
        #    - Start them at initial values of 0.0001 (testing shows this is ~10m);
        #    - For each threshold value, go through list of points p and test whether
        #      p +- threshold_value are both > cluster_distance;
        #    - If not, increment value. 
        # 
        #    This guarantees the following property at the end: 
        #    For any stop p, all other stops p' within cluster_distance 
        #    are within [p += lat_thresh, p+= lon_thresh]. 
        lat_thresh = 0.0001   
        lon_thresh = 0.0001 
        increment = 0.0001 

        # TODO (Future): Do both steps in one pass. 
        # Find proper lat_thresh value. 
        for stop in stops: 
            lat = stops_df.loc[stops_df.tag==stop, "lat"].values[0] 
            lon = stops_df.loc[stops_df.tag==stop, "lon"].values[0]  
            p = (lat,lon)   

            p_plus = (p[0] + lat_thresh, p[1]) 
            p_minus = (p[0] - lat_thresh, p[1])  
            dist_plus = calculate_distance_from_lat_lon_coords(p, p_plus) 
            dist_minus = calculate_distance_from_lat_lon_coords(p, p_minus) 

            dist = min(dist_plus, dist_minus) 

            # Increment threshold value as needed. 
            while dist <= cluster_distance:  
                lat_thresh += increment 

                p_plus = (p[0] + lat_thresh, p[1]) 
                p_minus = (p[0] - lat_thresh, p[1])  
                dist_plus = calculate_distance_from_lat_lon_coords(p, p_plus) 
                dist_minus = calculate_distance_from_lat_lon_coords(p, p_minus) 

                dist = min(dist_plus, dist_minus) 

        # Repeat for lon_thresh value. 
        for stop in stops: 
            lat = stops_df.loc[stops_df.tag==stop, "lat"].values[0] 
            lon = stops_df.loc[stops_df.tag==stop, "lon"].values[0]  
            p = (lat,lon)   

            p_plus = (p[0], p[1] + lon_thresh) 
            p_minus = (p[0], p[1] - lon_thresh)  
            dist_plus = calculate_distance_from_lat_lon_coords(p, p_plus) 
            dist_minus = calculate_distance_from_lat_lon_coords(p, p_minus) 

            dist = min(dist_plus, dist_minus) 

            # Increment threshold value as needed. 
            while dist <= cluster_distance:  
                lon_thresh += increment  

                p_plus = (p[0] + lon_thresh, p[1]) 
                p_minus = (p[0] - lon_thresh, p[1])  
                dist_plus = calculate_distance_from_lat_lon_coords(p, p_plus) 
                dist_minus = calculate_distance_from_lat_lon_coords(p, p_minus) 

                dist = min(dist_plus, dist_minus) 


        # 2. For each stop in list, search its neighborhood for nearby stops.
        for stop1 in stops:

            lat1 = stops_df.loc[stops_df.tag==stop1, "lat"].values[0]
            lon1 = stops_df.loc[stops_df.tag==stop1, "lon"].values[0]
            p1 = (lat1, lon1) 

            condition = (
                         (stops_df.lat.between(lat1-lat_thresh, lat1+lat_thresh)) 
                        &(stops_df.lon.between(lon1-lon_thresh, lon1+lon_thresh))
                        )  
            nhbd = stops_df[condition].tag.unique() 

            # Remove stop1 from its own neighborhood. 
            # We don't want it in the search area. 
            nhbd = [x for x in nhbd if x != stop1] 

            for stop2 in nhbd:  

                lat2 = stops_df.loc[stops_df.tag==stop2, "lat"].values[0]
                lon2 = stops_df.loc[stops_df.tag==stop2, "lon"].values[0]
                p2 = (lat2, lon2)     

                dist = calculate_distance_from_lat_lon_coords(p1, p2)
                if dist <= cluster_distance: 

                    data =  {
                        "key": "_".join([stop1, stop2]), 
                        "stop1": stop1,
                        "lat1": lat1,
                        "lon1": lon1,
                        "stop2": stop2, 
                        "lat2": lat2,
                        "lon2": lon2,   
                        "distance_meters": dist 
                    }
                    df_pairs_list.append(pd.DataFrame(data, index=[0])) 


        df_connections = pd.concat(df_pairs_list)  
        df_connections.reset_index(drop=True, inplace=True) 

        # Type validation and conversion 
        df_connections = df_connections.astype(connections_types) 

        return df_connections

    def populate_transit_graph_table(self):
        """Assemble the transit graph table from the stops and connections table.
        
        We construct a directed graph with the following types of edges:
            - consecutive stops on a direction;
            - stops in a connection.
        """

        # For each direction, build an edge dataframe and insert into db.
        agency_tag = self.db.get_agency_tag() 
        direction_tags = self.db.get_direction_list(agency_tag=agency_tag) 

        for tag in direction_tags: 

            df_direction_edges = self._build_direction_edges_df_from_database(
                                                                direction_tag=tag)  

            self.db.insert_dataframe_in_table("transit_graph", df_direction_edges) 

        # Transfer the connections table, adding the is_connection attribute. 
        self._add_connections_to_transit_graph_table() 

    def _build_direction_edges_df_from_database(self, direction_tag):
        """Construct part of the transit directed graph associated to a direction.
        For each consecutive stops s1, s2 on a direction, we add the edge s1 -> s2 
        to the dataframe. 

        Note: Since stop tags may have special endings such as _IB, _OB, _ar, 
        we store both the tag (for linking between tables) as well as its trimmed 
        version since there is only one actual node.   

        The dataframe created has column format: 
            - key (str),
            - stop_tag1 (str),
            - stop_tag2 (str),
            - node1 (str), 
            - node2 (str), 
            - direction_tag (str) 

        Args:
            direction_tag (str): Tag to fetch stops on a route direction.

        Returns:
            dataframe: Dataframe of consecutive stops on a direction. 
        """

        agency_tag = self.db.get_agency_tag()
        stops_df = self.db.get_stops_on_direction_dataframe(
                                            direction_tag=direction_tag,
                                            agency_tag=agency_tag
                                            )

        direction_edges_types = {
            "key": "str",
            "stop_tag1": "str",
            "stop_tag2": "str", 
            "node1": "str",
            "node2": "str", 
            "direction_tag": "str" 
        }

        rows = [] 
        num_stops = stops_df.shape[0]  
        for n in range(1, num_stops): 

            stop = stops_df.loc[stops_df.stop_along_direction==n,"stop_tag"].values[0]
            next_stop = stops_df.loc[stops_df.stop_along_direction==n+1,"stop_tag"].values[0]

            data = {}  
            data["stop_tag1"] = stop
            data["stop_tag2"] = next_stop  
            data["key"] = "_".join([stop, next_stop, direction_tag])
            data["direction_tag"] = direction_tag 

            rows.append(data) 

        df_direction_edges = pd.DataFrame(rows) 
        df_direction_edges = self._trim_stop_tags(df_direction_edges) 
        df_direction_edges = df_direction_edges.astype(direction_edges_types)

        return df_direction_edges 

    def _add_connections_to_transit_graph_table(self):
        """Add all transit graph edges coming from connections.
        """

        # Each connection gives a pair of directed edges in the transit graph.
        # We identify which ones come from such a connection.
        connections_df = self.db.get_connections_dataframe() 
        connections_df["is_connection"] = True

        # Some stop tags have additional endings (i.e. 1000 vs 1000_ar).
        # These stops are the same and the ending refers to the direction
        # the stop is on. We remove them when considering stops as nodes. 
        connections_df.rename(
            columns={"stop1": "stop_tag1", "stop2": "stop_tag2"},
            inplace=True) 
        connections_df = self._trim_stop_tags(connections_df)  

        df = connections_df[["key", "stop_tag1", "stop_tag2", 
                             "node1", "node2", "is_connection"]]  
        self.db.insert_dataframe_in_table("transit_graph", df) 

    def _trim_stop_tags(self, df):
        """Helper function. Used when assembling the transit graph from stops data.
        
        Stop tags sometimes have additional endings such as _IB, _OB, _ar, indicating
        whether the route direction considers the stop as inbound only, outbound only 
        or arrival only. Since these depend endings have meaning only with respect to 
        the direction, but the stop is otherwise the same (i.e. 1000 and 1000_IB are 
        the same stop), we remove them when considering stops as nodes in our graph. 
        """

        df["node1"] = df["stop_tag1"].str.replace("_IB","").str.replace("_OB","").str.replace("_ar","")
        df["node2"] = df["stop_tag2"].str.replace("_IB","").str.replace("_OB","").str.replace("_ar","")

        return df 

    def get_predicted_times_at_stops_df(self):

        trips_df = self._load_daily_trips_data()
        stops_df = self._load_stops_data()

        # We use a simple knn as regressor. 
        knn = KNeighborsRegressor(n_neighbors=3, p=1, weights="distance")

        df_list = []  
        groups = trips_df.groupby(["vehicle_id", "direction_tag", "trip_number"])
        for name, group_df in groups: 

            vehicle_id, direction_tag, trip_number = name
            group_stops_df = stops_df[stops_df.direction_tag==direction_tag]

            try:
                # Fit knn to vehicle's location data during trip. 
                X = group_df[["lat", "lon"]] 
                y = pd.to_numeric(group_df["read_time"]) 
                knn.fit(X, y)  

                # Then predict time-of-visit at stops. 
                times_df = group_stops_df[["lat", "lon", "stop_order"]]

                X_new = group_stops_df[["lat", "lon"]]  
                times_df["read_time"] = knn.predict(X_new) 
                times_df["read_time"] = pd.to_datetime(times_df["read_time"])

                # Tag the trip. 
                times_df["vehicle_id"] = vehicle_id
                times_df["direction_tag"] = direction_tag
                times_df["trip_number"] = trip_number 

                df_list.append(times_df) 

            except ValueError:  # this is from trips with n_sample < n_neighbors 
                pass            # these are not legitimate trips, so are ignored 

        return pd.concat(df_list) if df_list else None

    def _load_daily_trips_data(self):
        """Load daily vehicle locations data, prepared and segmented by trips."""

        queries = get_queries_path() 
        sql_file = f"{queries}/preparation_for_time_prediction_at_stops.sql"

        with self.db.connect() as conn: 
            with open(sql_file) as stmt:
                df = pd.read_sql(stmt.read(), conn)
            
        return df

    def _load_stops_data(self):
        """Load location and direction data for all stops."""

        queries = get_queries_path() 
        sql_file = f"{queries}/get_all_stops_data.sql"

        with self.db.connect() as conn: 
            with open(sql_file) as stmt:
                df = pd.read_sql(stmt.read(), conn)

        return df


class ResponseParser:

    def __init__(self):
        pass

    def parse_route_list_response_into_df_dict(self, response_dict,
                                               agency_tag): 
        """Parse the json response data coming from the routeList NextBus 
        API endpoint into a dataframe. By convention, the dataframe format 
        matches the 'routes' table for insertion. 

        The columns are: 
            - tag (str)
            - title (str)
            - latmin (float)
            - latmax (float)
            - lonmin (float)
            - lonmax (float)
            - agency_tag (str)

        Moreover, the four latmin-lonmax columns returned are Null. 
        Their values must be extracted from the 'routeConfig' endpoint.

        Returned dataframe is None if route data cannot be parsed.  

        Args:
            response_dict (dict): json response data from routeList endpoint.
            agency_tag (str): shortname of the corresponding agency (e.g. 'ttc') 

        Returns:
            df_dict: A single dataframe wrapped in a dict, with database 
            table name as key. 
        """

        # We'll first construct a null dataframe df_routes of the correct 
        # format and column types. We'll then extract the 'tag' & 'title' 
        # values from the response json, then add the agency_tag passed 
        # as an arg. 
        df_routes = None 
        routes_types = {
            "tag": "str", 
            "title": "str",
            "latmin": "float",
            "latmax": "float",
            "lonmin": "float",
            "lonmax": "float",
            "agency_tag": "str" 
        }

        # First check if we can extract a dataframe out of a response subdict.  
        # This doubles as a first format validation. 
        df_response = None 
        try: 
            routes = response_dict['route'] 
            df_response = pd.DataFrame(routes, index=range(len(routes))) 

        except:
            pass

        # Then build our dataframe from the response df, extracting values
        # and validating data types.   
        if df_response is not None: 

            num_rows = df_response.shape[0] 
            df_routes = pd.DataFrame(columns=routes_types.keys(),
                                     index=range(num_rows))  


            with contextlib.suppress(KeyError):  # tag
                df_routes["tag"] = df_response["tag"] 

            with contextlib.suppress(KeyError):  # title
                df_routes["title"] = df_response["title"] 

            df_routes["agency_tag"] = agency_tag 

            # Data validation: test and convert types as per template.
            df_routes = df_routes.astype(routes_types) 

        df_dict = {'routes': df_routes} 
        return df_dict

    def parse_route_config_response_into_df_dict(self, response_dict,
                                                 route_tag, agency_tag):
        """Parse the json response data coming from the routeConfig NextBus 
        API endpoint into a dataframe dict. By convention, each dataframe 
        matches the format of its intended database table for insertion. 
        The dict keys are the table names.  

        Three dataframes are returned: df_routes, df_directions, df_stops.

        Column formats are as follows. 

        df_routes:
            - tag (str)  
            - title (str)
            - latmin (float)
            - latmax (float)
            - lonmin (float)
            - lonmax (float)
            - agency_tag (str)

        df_directions:
            - tag (str),
            - title (str),
            - name (str),
            - route_tag (str),
            - branch (str),
            - agency_tag (str)

        df_stops: 
            - tag (str),
            - title (str),
            - lat (float),
            - lon (float),
            - route_tag (str),
            - direction_tag (str),
            - stop_along_direction (int),
            - key (str), 
            - agency_tag (str) 

        A returned dataframe is None if the corresponding data cannot be parsed.  

        Args:
            response_dict (dict): json response data from routeConfig endpoint.
            route_tag (str): route number corresponding to config. 
            agency_tag (str): shortname of the corresponding agency (e.g. 'ttc') 

        Returns:
            df_dict: Three dataframes wrapped in a dict, with corresponding 
            database table name as key.  
        """

        df_routes = self._get_df_routes_from_route_config_response(
                                            response_dict=response_dict,
                                            route_tag=route_tag,
                                            agency_tag=agency_tag
                                            )
        df_directions = self._get_df_directions_from_route_config_response(
                                            response_dict=response_dict,
                                            route_tag=route_tag,
                                            agency_tag=agency_tag
                                            )
        df_stops = self._get_df_stops_from_route_config_response(
                                            response_dict=response_dict,
                                            route_tag=route_tag,
                                            agency_tag=agency_tag
                                            )

        df_dict = {
            "routes": df_routes,
            "directions": df_directions,
            "stops": df_stops 
        }
        return df_dict

    def _get_df_routes_from_route_config_response(self, response_dict, 
                                                  route_tag, agency_tag):
        """Helper function to parse_route_config_response_into_df_dict.

        Args:
            response_dict (dict): json response data from routeConfig endpoint.
            route_tag (str): route number corresponding to config. 
            agency_tag (str): shortname of the corresponding agency (e.g. 'ttc') 

        Returns:
            df_routes: Dataframe with format matching the 'routes' table.
        """

        # First, we construct null dataframes of the correct column format.
        # We'll then populate these from the response data, validating
        # and converting types.   
        df_routes = None
        routes_types = {
            "tag": "str",
            "title": "str",
            "latmin": "float",
            "latmax": "float",
            "lonmin": "float",
            "lonmax": "float",
            "agency_tag": "str"
        }

        df_response = None
        try:  
            route = response_dict['route'] 

            keys = ["title", "latMin", "latMax", "lonMin", "lonMax"]
            route = {k: v for k,v in route.items() if k in keys}  
            df_response = pd.DataFrame(route, index=[0])  

        except:
            pass

        if df_response is not None: 

            # Init null df with the correct format. 
            num_rows = df_response.shape[0]
            df_routes = pd.DataFrame(columns=routes_types.keys(),
                                     index=range(num_rows))

            # Build df_routes from the response df.
            df_routes["tag"] = route_tag 
            df_routes["agency_tag"] = agency_tag
 
            with contextlib.suppress(KeyError):  # title
                df_routes["title"] = df_response["title"] 

            with contextlib.suppress(KeyError):  # latmin
                df_routes["latmin"] = df_response["latMin"] 

            with contextlib.suppress(KeyError):  # latmax
                df_routes["latmax"] = df_response["latMax"] 

            with contextlib.suppress(KeyError):  # lonmin
                df_routes["lonmin"] = df_response["lonMin"]   

            with contextlib.suppress(KeyError):  # lonmax
                df_routes["lonmax"] = df_response["lonMax"]    
                
            # Validate and convert data types. 
            df_routes = df_routes.astype(routes_types) 

            # Finally, reorder columns so they match the database order.
            col_order = list(routes_types.keys()) 
            df_routes = df_routes[col_order]  

        return df_routes 

    def _get_df_directions_from_route_config_response(self, response_dict, 
                                                     route_tag, agency_tag):
        """Helper function to parse_route_config_response_into_df_dict.

        Args:
            response_dict (dict): json response data from routeConfig endpoint.
            route_tag (str): route number corresponding to config. 
            agency_tag (str): shortname of the corresponding agency (e.g. 'ttc') 

        Returns:
            df_directions: Dataframe with format matching the 'directions' table.
        """

        # First, we construct null dataframes of the correct column format.
        # We'll then populate these from the response data, validating
        # and converting types.   
        df_directions = None
        directions_types = {
            "tag": "str",
            "title": "str",
            "name": "str",
            "route_tag": "str",
            "branch": "str",
            "agency_tag": "str"
        }

        df_response = None
        try:
            directions = response_dict['route']['direction']  # list of dicts

            # We filter each direction dict to the keys we need. 
            keys = ["tag", "title", "name", "branch"]  

            dct_map = map(
                        lambda x: {k:v for k,v in x.items() if k in keys}, 
                        directions
                        ) 
            dct_list = list(dct_map)

            # Some keys can be missing from a dict (e.g. 'branch' for short directions).
            # We fill them out to null.
            for dct in dct_list:  
                for key in keys:
                    if key not in dct:
                        dct[key] = None 

            df_response = pd.DataFrame(dct_list)  

        except:
            pass

        if df_response is not None: 

            # Init null dataframe. 
            num_rows = df_response.shape[0] 
            df_directions = pd.DataFrame(columns=directions_types.keys(),
                                         index=range(num_rows)) 

            # Extract values from response df.

            df_directions["route_tag"] = route_tag      # passed as arg
            df_directions["agency_tag"] = agency_tag    # passed as arg

            with contextlib.suppress(KeyError):
                df_directions["tag"] = df_response["tag"] 

            with contextlib.suppress(KeyError):
                df_directions["title"] = df_response["title"] 

            with contextlib.suppress(KeyError):
                df_directions["name"] = df_response["name"] 

            with contextlib.suppress(KeyError):
                df_directions["branch"] = df_response["branch"]  


            # Validate and convert data types. 
            df_directions = df_directions.astype(directions_types)

            # Finally, reorder columns so they match the database order.
            col_order = list(directions_types.keys()) 
            df_directions = df_directions[col_order] 

        return df_directions 

    def _get_df_stops_from_route_config_response(self, response_dict, 
                                                 route_tag, agency_tag):
        """Helper function to parse_route_config_response_into_df_dict.

        Args:
            response_dict (dict): json response data from routeConfig endpoint.
            route_tag (str): route number corresponding to config. 
            agency_tag (str): shortname of the corresponding agency (e.g. 'ttc') 

        Returns:
            df_stops: Dataframe with format matching the 'stops' table.
        """

        # We construct null dataframes of the correct column format. 
        # We'll then populate these from the response data, validating
        # and converting types.   
        df_stops = None 
        stops_types = {
            "tag": "str",
            "title": "str",
            "lat": "float",
            "lon": "float",
            "route_tag": "str",
            "direction_tag": "str",
            "stop_along_direction": "int",
            "key": "str", 
            "agency_tag": "str"
        }

        # First, extract the stops data from the stop key, without direction tag.
        # Then, join the direction tag from the direction key to each stop. 
        # This last thing will let us record the order of each stop on a direction. 
        df_response = None 
        try:  # extract stop data, without direction_tag
            stops = response_dict['route']['stop']  # list of dicts

            keys = ["tag", "title", "lat", "lon"] 
            dct_map = map(
                        lambda x: {k: v for k,v in x.items() if k in keys},
                        stops
                        )
            dct_list = list(dct_map) 

            df_response = pd.DataFrame(dct_list) 

        except:
            pass

        try:  # extract all (stop_tag, direction_tag) pairs and stop order number
            directions = response_dict['route']['direction']  # list of dicts 

            df_list = [] 
            for direction in directions:
                stops_data = direction['stop']    # list of dicts  
                direction_tag = direction['tag']  # single tag

                for stop_number, dct in enumerate(stops_data):
                    dct["direction_tag"] = direction_tag 
                    dct["stop_along_direction"] = stop_number + 1  # start at 1

                df_list.append(pd.DataFrame(stops_data))

            df_tag_pairs = pd.concat(df_list)   

            # Test that at least some stops in our df_response have an 
            # accompanying direction tag. Otherwise we should return
            # a null dataframe, since this means an endpoint or parsing issue. 
            if df_response is not None:
                stops_list1 = df_response["tag"].unique()
                stops_list2 = df_tag_pairs["tag"].unique()

                assert(any( [tag in stops_list1 for tag in stops_list2] ))

        except:  # if no stop can be matched to a direction, null df_response 
            df_response = None 

        if df_response is not None: 

            # Init null df.
            num_rows = df_response.shape[0] 
            df_stops = pd.DataFrame(data=None, index=range(num_rows))

            for column in ["tag", "title", "lat", "lon"]: 
                df_stops[column] = None

            # Extraction values from response. 
            df_stops["route_tag"] = route_tag       # passed from arg
            df_stops["agency_tag"] = agency_tag     # passed from arg

            with contextlib.suppress(KeyError):     # tag
                df_stops["tag"] = df_response["tag"]

            with contextlib.suppress(KeyError):     # title
                df_stops["title"] = df_response["title"]

            with contextlib.suppress(KeyError):     # lat
                df_stops["lat"] = df_response["lat"]

            with contextlib.suppress(KeyError):     # lon
                df_stops["lon"] = df_response["lon"] 

            # Left join direction_tag to each stop.  
            df_stops = pd.merge(df_stops, df_tag_pairs, how="left",
                                left_on="tag",
                                right_on="tag",
                                ) 

            # Add primary key: concatenation of stop_tag and direction_tag
            df_stops["key"] = df_stops["tag"]+"_"+df_stops["direction_tag"]

            # Validate and convert data types. 
            df_stops = df_stops.astype(stops_types) 

            # Finally, order columns as in the database. 
            col_order = list(stops_types.keys())
            df_stops = df_stops[col_order] 

        return df_stops

    def parse_schedule_response_into_df_dict(self, response_dict, 
                                             route_tag, agency_tag,
                                             time_of_extraction):
        """Parse the json response data coming from the schedule NextBus 
        API endpoint into a dataframe. By convention, the dataframe format 
        matches the 'schedules' table for insertion.  

        The columns are:  
            - schedule_class (str),
            - service_class (str),
            - route_tag (str),
            - route_title (str),
            - direction_name (str),
            - block_id (str),
            - stop_tag (str),
            - epoch_time (int),
            - ETA (str),
            - agency_tag (str),
            - key (str),
            - last_extracted (datetime64[ns]) 

        Args:
            response_dict (dict): json response data from schedule endpoint.
            route_tag (str): route number corresponding to schedule.  
            agency_tag (str): shortname of the corresponding agency (e.g. 'ttc') 

        Returns: 
            df_dict: A single dataframe wrapped in a dict, with database 
            table name as key. 
        """
        
        # We construct a null dataframe of the correct format and types.
        # We'll then extract the values out of the schedule response,
        # validating and converting types as we go. 
        df_schedules = None
        schedules_types = {
            "schedule_class": "str",
            "service_class": "str",
            "route_tag": "str",
            "route_title": "str",
            "direction_name": "str",
            "block_id": "str",
            "stop_tag": "str",
            "epoch_time": "int",
            "ETA": "str",
            "agency_tag": "str",
            "key": "str",
            "last_extracted": "datetime64[ns]"
        } 

        # First, we try extracting something formated as a dataframe.
        # This provides a first format validation. 
        df_response = None 
        try:
            # The schedules response has the following structure:
            #
            #   a. The route key contains a list of schedules;
            #   b. Each schedule contains
            #       - constant data for the schedule (serviceClass, 
            #         serviceClass, route title, direction name);  
            #       - a list of timetable data, with multiple block ids;
            #   c. Each block id contains a table of stop tags, epoch times
            #      and ETAs. 
            #
            # We will work upward from the bottom-most nested subdict:
            #   
            #   1. For each block_id in a schedule, extract the timetable;
            #   2. Concatenate all block_ids in a schedule;
            #   3. Join constant data to the schedule (serviceClass, and so on);
            #   4. Concatenate all schedules; 
            #
            # TODO: Rewrite this block of code (flagged by profiler).

            schedules = response_dict['route'] 
            
            schedule_class_df_list = [] 
            for schedule_class in schedules:  # service classes (e.g. holidays, sun, sat)

                block_id_df_list = []
                blocks = schedule_class["tr"]    # block: ~bus run
                for block in blocks:
                    block_id = block["blockID"]
                    block_stops = block["stop"]  # list of dicts 

                    block_id_df = pd.DataFrame(block_stops)  # timetable
                    block_id_df["block_id"] = block_id       # stamp it

                    block_id_df_list.append(block_id_df) 

                schedule_df = pd.concat(block_id_df_list)  # schedule for given class

                schedule_df["schedule_class"] = schedule_class["scheduleClass"] 
                schedule_df["service_class"] = schedule_class["serviceClass"]  
                schedule_df["route_title"] = schedule_class["title"] 
                schedule_df["direction_name"] = schedule_class["direction"] 

                schedule_class_df_list.append(schedule_df) 


            df_response = pd.concat(schedule_class_df_list)   # all schedules for route 
            df_response.reset_index(drop=True, inplace=True)  # for access issues 

        except:
            pass

        # Then assemble our dataframe from the response df,
        # validating and converting types. 
        if df_response is not None:

            # Init null df.
            num_rows = df_response.shape[0]  
            df_schedules = pd.DataFrame(columns=schedules_types.keys(),
                                        index=range(num_rows))

            # Extract values. 
            df_schedules["route_tag"] = route_tag    # passed as arg
            df_schedules["agency_tag"] = agency_tag  # passed as arg
            df_schedules["last_extracted"] = time_of_extraction  # passed as arg  

            with contextlib.suppress(KeyError):  # schedule_class
                df_schedules["schedule_class"] = df_response["schedule_class"]

            with contextlib.suppress(KeyError):  # service_class
                df_schedules["service_class"] = df_response["service_class"]

            with contextlib.suppress(KeyError):  # route_title
                df_schedules["route_title"] = df_response["route_title"] 

            with contextlib.suppress(KeyError):  # direction_name
                df_schedules["direction_name"] = df_response["direction_name"]  

            with contextlib.suppress(KeyError):  # block_id
                df_schedules["block_id"] = df_response["block_id"]

            with contextlib.suppress(KeyError):  # stop_tag
                df_schedules["stop_tag"] = df_response["tag"]   

            with contextlib.suppress(KeyError):  # epoch_time
                df_schedules["epoch_time"] = df_response["epochTime"] 

            with contextlib.suppress(KeyError):  # ETA
                df_schedules["ETA"] = df_response["content"]    

            # Add primary key. We'll concatenate the pandas index to the route tag.
            df_schedules.reset_index(drop=True, inplace=True)
            df_schedules["key"] = str(route_tag) + "_" 
            df_schedules["key"] += df_schedules["schedule_class"] + "_"

            # We leftpad the index digits by zeros up to above their max length.
            pad_width = 8  # typical indices run up to 5, we're being generous here

            df_schedules["key"] += df_schedules.index.astype("str").str.pad( 
                                                                width=pad_width,
                                                                fillchar='0'
                                                                )

            # Validate and convert data types. 
            df_schedules = df_schedules.astype(schedules_types) 

            # Finally, order columns as in the database. 
            col_order = list(schedules_types.keys()) 
            df_schedules = df_schedules[col_order]

        df_dict = {"schedules": df_schedules}
        return df_dict

    def parse_vehicle_locations_response_into_df_dict(self, response_dict, 
                                                      agency_tag, time_of_extraction):
        """Parse the json response data coming from following NextBus API 
        endpoint into a dataframe:
            - vehicleLocations 
            - vehicleLocation

        By convention, the dataframe format matches the 'vehicle_locations' 
        table for insertion.  

        The columns are:  
            - route_tag (str),
            - predictable (bool),
            - heading (int),
            - speed_kmhr (int),
            - lat (float),
            - lon (float),
            - id (str),
            - direction_tag (str),
            - agency_tag (str),
            - read_time (datetime64[ns]),
            - key (str) 

        Args:
            response_dict (dict): json response data from vehicleLocation(s) endpoint.
            agency_tag (str): shortname of the corresponding agency (e.g. 'ttc') 

        Returns: 
            df_dict: A single dataframe wrapped in a dict, with database 
            table name as key. 
        """
        # First, each vehicle log has a "secsSinceReport" attribute. We use this
        # to filter out old readings (>30 min ago), and only query active vehicles.
        # We do this for the single vehicle endpoint only.
        try:
            vehicle = response_dict["vehicle"]
            if type(vehicle) is dict:
                secs_since_report = int(vehicle["secsSinceReport"])
                if secs_since_report >= 1800:
                    return {"vehicle_locations": None}
        except:
            pass

        # We also use the "secsSinceReport" attribute to pinpoint vehicle log time,
        # by substracting it from current time.
        now = time_of_extraction

        # We construct a null dataframe of the correct format and types.
        # We'll then extract the values out of the schedule response,
        # validating and converting types as we go. 
        df_vehicle_locations = None
        vehicle_locations_types = {
            "route_tag": "str",
            "predictable": "bool",
            "heading": "int",
            "speed_kmhr": "int",
            "lat": "float",
            "lon": "float",
            "id": "str",
            "direction_tag": "str",
            "agency_tag": "str",  
            "read_time": "datetime64[ns]",
            "key": "str"
        } 

        # First check if we can extract a dataframe out of a response subdict.  
        # This doubles as a first format validation. 
        df_response = None
        try:
            vehicle = response_dict["vehicle"] 

            # The expected format of vehicle depends on the endpoint:
            # vehicleLocation: dict
            # vehicleLocations: list of dicts
            if type(vehicle) is dict:
                df_response = pd.DataFrame(vehicle, index=[0]) 
            elif type(vehicle) is list:
                df_response = pd.DataFrame(vehicle) 

        except:
            pass

        if df_response is not None:

            num_rows = df_response.shape[0]
            df_vehicle_locations = pd.DataFrame(columns=vehicle_locations_types.keys(), 
                                                index=range(num_rows))

            for column in vehicle_locations_types.keys():
                df_vehicle_locations[column] = None

            df_vehicle_locations["agency_tag"] = agency_tag  # passed as arg

            with contextlib.suppress(KeyError):  # route_tag 
                df_vehicle_locations["route_tag"] = df_response["routeTag"]

            with contextlib.suppress(KeyError):  # predictable
                df_vehicle_locations["predictable"] = df_response["predictable"]
            
            with contextlib.suppress(KeyError):  # heading
                df_vehicle_locations["heading"] = df_response["heading"]

            with contextlib.suppress(KeyError):  # speed_kmhr 
                df_vehicle_locations["speed_kmhr"] = df_response["speedKmHr"]

            with contextlib.suppress(KeyError):  # lat
                df_vehicle_locations["lat"] = df_response["lat"]

            with contextlib.suppress(KeyError):  # lon
                df_vehicle_locations["lon"] = df_response["lon"] 

            with contextlib.suppress(KeyError):  # id
                df_vehicle_locations["id"] = df_response["id"] 

            with contextlib.suppress(KeyError):  # direction_tag
                df_vehicle_locations["direction_tag"] = df_response["dirTag"] 

            # Calculate the time at which vehicle sensor read was taken.
            with contextlib.suppress(KeyError):  # read_time
                secs_since_report = pd.to_timedelta(df_response["secsSinceReport"].values.astype("int"),
                                                    unit="seconds")
                #secs_since_report = df_response["secsSinceReport"].astype("int").apply(
                #                        lambda x: datetime.timedelta(seconds=x)
                #                        )
                df_vehicle_locations["read_time"] = now - secs_since_report

            # Primary key is the concatenation of vehicle id and read_time.
            # To avoid duplicates, we round read_time to the nearest 1/6th min; this is so
            # vehicles reporting the same data in subsequent queries (i.e. where secsSinceReport
            # has increased by 5 minutes when queried 5 minutes later) are only given
            # a single primary key.  
            if df_vehicle_locations["read_time"] is not None:
                vehicle_id = df_vehicle_locations["id"] 
                read_time = df_vehicle_locations["read_time"].apply(str).str.slice(stop=-10)
                #read_time = read_time.apply(lambda x: "".join([x[:-8], "0"]))  
                #read_time = read_time.apply(lambda x: x[:-10]) 
                df_vehicle_locations["key"] = vehicle_id + "_" + read_time 

            # Type validation.
            df_vehicle_locations = df_vehicle_locations.astype(vehicle_locations_types)

            # We order columns as in the database.
            col_order = list(vehicle_locations_types.keys())
            df_vehicle_locations = df_vehicle_locations[col_order] 

        df_dict = {"vehicle_locations": df_vehicle_locations} 
        return df_dict 
