"""
Load various configs from environment variables 
"""
import os

def get_db_config():
    config = {} 
    config["db_type"] = os.environ["DB_CONFIG_DB_TYPE"]
    config["con"] = os.environ["DB_CONFIG_CON"] 
    config["host"] = os.environ["DB_CONFIG_HOST"] 
    config["usr"] = os.environ["DB_CONFIG_USR"]
    config["db"] = os.environ["DB_CONFIG_DB"]
    config["pw"] = os.environ["DB_CONFIG_PW"] 
    return config

def get_transit_config():
    config = {} 
    config["agency_tag"] = os.environ["TRANSIT_CONFIG_AGENCY_TAG"] 
    config["connections_cluster_max_distance_meters"] = int(os.environ["TRANSIT_CONFIG_CONNECTIONS_CLUSTER_MAX_DISTANCE_METERS"])
    return config

def get_pipeline_config():
    config = {} 
    config["vehicle_locations_retention_days"] = int(os.environ["PIPELINE_CONFIG_VEHICLE_LOCATIONS_RETENTION_DAYS"])
    return config 

def get_ssh_tunnel_config():
    """Return config to ssh tunnel to aws EC2 instance from local."""
    config = {} 

    # Format as ('ssh_address': str, port: int). Environment variable is stored as a csv string.
    config["ssh_address_or_host"] = os.environ["SSH_TUNNEL_CONFIG_SSH_ADDRESS_OR_HOST"].split(",") 
    config["ssh_address_or_host"][1] = int(config["ssh_address_or_host"][1]) 
    config["ssh_address_or_host"] = tuple(config["ssh_address_or_host"]) 

    config["ssh_username"] = os.environ["SSH_TUNNEL_CONFIG_SSH_USERNAME"] 
    config["ssh_pkey"] = os.environ["SSH_TUNNEL_CONFIG_SSH_PKEY"] 

    # Format as ('bind_address': str, port: int).  
    config["remote_bind_address"] = os.environ["SSH_TUNNEL_CONFIG_REMOTE_BIND_ADDRESS"].split(",")
    config["remote_bind_address"][1] = int(config["remote_bind_address"][1])  
    config["remote_bind_address"] = tuple(config["remote_bind_address"]) 
    return config

