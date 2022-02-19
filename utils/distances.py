"""
Function to calculate distances between points in (lat, lon) coordinates.
"""
import math

def calculate_distance_from_lat_lon_coords(p1, p2):    
    """Calculate meter distances between two points p1, p2 
    given in (latitude, longitude) coordinates. 
    
    The calculation uses the Haversine formula, see 
    https://www.kite.com/python/answers/how-to-find-the-distance-between-two-lat-long-coordinates-in-python


    Args:
        p1, p2 (float tuple): Points given in (lat, lon) coordinates.

    Returns:
        distance (float): Distance in meters.
    """

    if p1 == p2:
        return 0 

    R = 6373.0  # Earth's radius in km

    lat1 = math.radians(p1[0]) 
    lon1 = math.radians(p1[1]) 

    lat2 = math.radians(p2[0]) 
    lon2 = math.radians(p2[1]) 

    dlon = lon2 - lon1
    dlat = lat2 - lat1 

    # Haversine Formula
    a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a)) 
    distance_km = R * c  

    distance = distance_km*1000 
    return distance

