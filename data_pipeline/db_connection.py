"""
Create sqlalchemy engines and connections with credentials to talk to the database. 
"""
import sqlalchemy
from sqlalchemy.orm import sessionmaker 
from utils.configs import get_db_config


def create_engine():
    """Wrapper for the sqlalchemy.create_engine function, instantiated
    with the database config.  

    Returns:
        engine: sqlalchemy Engine object.  
    """
    arg = "{db_type}+{con}://{usr}:{pw}@{host}/{db}"
    return sqlalchemy.create_engine(arg.format(**get_db_config())) 

def create_session(): 
    """Create a sqlalchemy.orm session to talk to the database. 

    Returns:
        session: sqlalchemy.orm session instance. 
    """
    engine = create_engine() 
    Session = sessionmaker(bind=engine) 
    return Session() 
