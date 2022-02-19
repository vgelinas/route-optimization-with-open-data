"""  
Context manager for an AWS EC2 database remote connection.
""" 
import sqlalchemy
from sshtunnel import SSHTunnelForwarder
from utils.configs import get_db_config, get_ssh_tunnel_config


class Connection: 
    """
    Context manager for an AWS EC2 database remote connection.

    Usage: 
        with Connection() as conn:
            df = pd.read_sql(your_query_string_here, conn)
    """

    def __init__(self): 
        self.tunnel = SSHTunnelForwarder(**get_ssh_tunnel_config())

    def __enter__(self):
        self.tunnel.__enter__()
        self.conn = None
        try:
            db_config = get_db_config()
            db_config["port"] = str(self.tunnel.local_bind_port)

            self.conn = sqlalchemy.create_engine(
                "{db_type}+{con}://{usr}:{pw}@{host}:{port}/{db}".format(
                **db_config)
                ).connect()
            return self.conn

        except Exception as e:
            self.tunnel.__exit__(None, None, None)
            raise e

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            self.conn.__exit__(exc_type, exc_value, traceback)
        finally:
            self.tunnel.__exit__(exc_type, exc_value, traceback) 
