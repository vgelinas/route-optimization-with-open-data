"""Scripts to run various pipeline components. 
"""
import argparse
from pipeline import Pipeline
from utils.configs import get_pipeline_config 


if __name__ == "__main__":

    parser = argparse.ArgumentParser() 
    parser.add_argument("-tc", "--transitConf", action="store_true",
                        help="download transit configuration tables")  
    parser.add_argument("-sc", "--schedules", action="store_true",
                        help="download schedules table")
    parser.add_argument("-cn", "--connections", action="store_true",
                        help="build connections between nearby stops") 
    parser.add_argument("-tg", "--transitGraph", action="store_true",
                        help="build transit graph table from config tables")
    parser.add_argument("-av", "--activeVehicles", action="store_true",
                        help="fetch snapshot of active vehicles over all routes") 
    parser.add_argument("-vl", "--vehicleLocations", action="store_true",
                        help="fetch current location data for all known vehicles")  
    parser.add_argument("-vvl", "--validationVehicleLocations", action="store_true",
                        help="fetch current location data for all validation vehicles")  
    parser.add_argument("-dv", "--deleteVehicles", action="store_true",
                        help="delete vehicle location data outside of retention period")
    parser.add_argument("-w", "--wait", type=int,
                        help="wait number of seconds between API calls") 
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="increase output verbosity") 
    args = parser.parse_args() 


    pipeline = Pipeline()

    if args.verbose:
        pipeline.data_loader.set_verbose(args.verbose)

    if args.wait:
        pipeline.data_loader.set_wait_time(args.wait)

    if args.transitConf: 
        pipeline.data_loader.populate_transit_config_tables_from_API()

    if args.schedules:
        pipeline.data_loader.populate_schedules_table_from_API()

    if args.connections:
        pipeline.data_preparation.populate_connections_table()

    if args.transitGraph:
        pipeline.data_preparation.populate_transit_graph_table()

    if args.activeVehicles:
        pipeline.data_loader.fetch_active_vehicles_snapshop_from_API() 

    if args.vehicleLocations:
        config = get_pipeline_config() 
        retention_period = config["vehicle_locations_retention_days"]  
        pipeline.data_loader.fetch_vehicle_locations_from_API(
                                        active_over_num_days=retention_period)

    if args.validationVehicleLocations:
        pipeline.data_loader.fetch_validation_vehicle_locations_from_API()

    if args.deleteVehicles:
        config = get_pipeline_config()
        retention_period = config["vehicle_locations_retention_days"]
        pipeline.data_loader.delete_old_vehicle_locations_entries(
                                        keep_num_days=retention_period) 
