"""
Generator utility function for producing API batch requests. 

Shamelessly stolen from https://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks
"""

def batches(lst, size):
    """Yield successive same-sized batches from list."""
    for i in range(0, len(lst), size):
        yield lst[i:i+size] 
