""" 
Utility methods for running SQL queries.
"""
import os

def get_queries_path():
    return os.environ["QUERIES_PATH"]