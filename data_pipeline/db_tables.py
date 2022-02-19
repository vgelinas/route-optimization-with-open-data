"""
Database table information for the sqlalchemy ORM.
"""
from sqlalchemy import Column
from sqlalchemy.types import Integer, Float, String, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()


# --------------------------- TABLES ---------------------------------------
class Agencies(Base):
    __tablename__ = 'agencies'

    id = Column(Integer, primary_key=True) 
    tag = Column(String(255))


class Routes(Base):
    __tablename__ = 'routes'

    tag = Column(String(255), primary_key=True, autoincrement=False)
    title = Column(String(255))
    latmin = Column(Float(32))
    latmax = Column(Float(32))
    lonmin = Column(Float(32))
    lonmax = Column(Float(32))
    agency_tag = Column(String(255)) 


class Directions(Base):
    __tablename__ = 'directions'

    tag = Column(String(255), primary_key=True, autoincrement=False)
    title = Column(String(255))
    name = Column(String(255))
    route_tag = Column(String(255)) 
    branch = Column(String(255)) 
    agency_tag = Column(String(255)) 


class Stops(Base):
    __tablename__ = 'stops' 

    tag = Column(String(255))  
    title = Column(String(255))
    lat = Column(Float(32)) 
    lon = Column(Float(32))
    route_tag = Column(String(255)) 
    direction_tag = Column(String(255)) 
    stop_along_direction = Column(Integer)  
    key = Column(String(255), primary_key=True, autoincrement=False)
    agency_tag = Column(String(255)) 


class Schedules(Base):
    __tablename__ = 'schedules'

    schedule_class = Column(String(255))
    service_class = Column(String(255))
    route_tag = Column(String(255))     
    route_title = Column(String(255))
    direction_name = Column(String(255))
    block_id = Column(String(255))
    stop_tag = Column(String(255))
    epoch_time = Column(Integer) 
    ETA = Column(String(255))
    agency_tag = Column(String(255))
    key = Column(String(255), primary_key=True) 
    last_extracted = Column(DateTime) 


class Vehicles(Base):
    __tablename__ = "vehicles"

    id = Column(String(255), primary_key=True) 
    last_seen_active = Column(DateTime) 
    agency_tag = Column(String(255)) 


class VehicleLocations(Base):
    __tablename__ = 'vehicle_locations'

    route_tag = Column(String(255))
    predictable = Column(Boolean)
    heading = Column(Integer)
    speed_kmhr = Column(Integer)
    lat = Column(Float(32)) 
    lon = Column(Float(32))
    id = Column(String(255))
    direction_tag = Column(String(255))
    agency_tag = Column(String(255))
    read_time = Column(DateTime)
    key = Column(String(255), primary_key=True) 


class VehiclesValidation(Base):
    __tablename__ = "vehicles_validation"

    id = Column(String(255), primary_key=True) 
    last_seen_active = Column(DateTime) 
    agency_tag = Column(String(255)) 


class VehicleLocationsValidation(Base):
    __tablename__ = 'vehicle_locations_validation'

    route_tag = Column(String(255))
    predictable = Column(Boolean)
    heading = Column(Integer)
    speed_kmhr = Column(Integer)
    lat = Column(Float(32)) 
    lon = Column(Float(32))
    id = Column(String(255))
    direction_tag = Column(String(255))
    agency_tag = Column(String(255))
    read_time = Column(DateTime)
    key = Column(String(255), primary_key=True) 


class Connections(Base):
    __tablename__ = 'connections'

    key = Column(String(255), primary_key=True) 
    stop1 = Column(String(255))
    lat1 = Column(Float)
    lon1 = Column(Float)
    stop2 = Column(String(255))  
    lat2 = Column(Float)
    lon2 = Column(Float) 
    distance_meters = Column(Float)  


class TransitGraph(Base):
    __tablename__ = 'transit_graph'
      
    key = Column(String(255), primary_key=True) 
    stop_tag1 = Column(String(255))
    stop_tag2 = Column(String(255))  
    node1 = Column(String(255))
    node2 = Column(String(255)) 
    direction_tag = Column(String(255)) 
    is_connection = Column(Boolean) 



