"""
Utility methods to handle streaming of large calculations.

Taken from https://maxhalford.github.io/blog/pandas-streaming-groupby/
"""
import pandas as pd


def stream_groupby_from_csv(path, keys, agg, store_result, chunk_size=1e6, **kwarg):

    # Make sure keys is a list of columns 
    if not isinstance(keys, list):
        keys = [keys] 

    # Tell pandas to read the data in chunks
    chunks = pd.read_csv(path, chunksize=chunk_size, **kwarg)
    orphans = pd.DataFrame() 

    for chunk in chunks:

        # Add the previous orphans to the chunk
        chunk = pd.concat((orphans, chunk))

        # Determine which rows are orphans
        last_val = chunk[keys].iloc[-1]
        is_orphan = (chunk[keys] == last_val).all(axis=1) 

        # Put the new orphans aside
        chunk, orphans = chunk[~is_orphan], chunk[is_orphan] 

        # Perform the groupby calculation and store result
        store_result(agg(chunk))

    # Don't forget the remaining orphans
    if len(orphans):
        store_result(agg(orphans)) 


def stream_groupby_from_db(query, conn, keys, agg, store_result, chunk_size=1e6, 
                           **kwarg):

    # Make sure keys is a list of columns 
    if not isinstance(keys, list):
        keys = [keys] 

    # Read data in chunks from db 
    with conn.execution_options(stream_results=True) as conn:

        chunks = pd.read_sql(query, conn, chunksize=chunk_size, **kwarg)
        orphans = pd.DataFrame() 

        for chunk in chunks:

            # Add the previous orphans to the chunk
            chunk = pd.concat((orphans, chunk))

            # Determine which rows are orphans
            last_val = chunk[keys].iloc[-1]
            is_orphan = (chunk[keys] == last_val).all(axis=1) 

            # Put the new orphans aside
            chunk, orphans = chunk[~is_orphan], chunk[is_orphan] 

            # Perform the groupby calculation and store result
            store_result(agg(chunk))

        # Don't forget the remaining orphans
        if len(orphans):
            store_result(agg(orphans)) 
