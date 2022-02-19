#!/bin/bash

# Activate environment variables
source /home/ubuntu/route-optimization-with-open-data/.venv/bin/activate 

# Run pipeline using .venv's python
/home/ubuntu/route-optimization-with-open-data/.venv/bin/python3 /home/ubuntu/route-optimization-with-open-data/data_pipeline/run_pipeline.py -dv 
