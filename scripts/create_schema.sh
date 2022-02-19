#!/bin/bash

# Activate environment variables
source /home/ubuntu/route-optimization-with-open-data/.venv/bin/activate 

# Run using .venv's python
/home/ubuntu/route-optimization-with-open-data/.venv/bin/python3 /home/ubuntu/route-optimization-with-open-data/data_pipeline/build_db.py
