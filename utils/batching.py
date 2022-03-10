"""
Generator utility method for batching API requests or resource-heavy jobs.
"""
from itertools import islice

def batches(iterable, batch_size):
    iterator = iter(iterable) 
    while batch := list(islice(iterator, batch_size)):
        yield batch
