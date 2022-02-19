"""
Scripts to create database schema and populate some initial tables. 
"""
import argparse
import db_connection
from db_tables import Base, Agencies
from sqlalchemy import inspect
from utils.configs import get_transit_config


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="increase output verbosity")
    args = parser.parse_args()

    # Create all tables which don't currently exist. 
    engine = db_connection.create_engine()
    session = db_connection.create_session()

    if args.verbose:
        # List existing tables in db.
        inspector = inspect(engine)
        existing_tables = inspector.get_table_names()
        all_tables = [table.name for table in Base.metadata.sorted_tables]
        missing_tables = [t for t in all_tables if t not in existing_tables]

        if missing_tables:
            print("="*60)
            print("Creating the following tables:")
            print(*[">> " + table for table in missing_tables], sep="\n")
            print("="*60)
        else:
            print("All tables already exist.")

    Base.metadata.create_all(engine, checkfirst=True)

    # The agencies table simply holds the tag for our chosen agency.
    config = get_transit_config()
    agency_tag = config["agency_tag"]  

    if not session.query(Agencies).first():
        agency = Agencies(tag=agency_tag)   
        session.add(agency) 
        session.commit()

