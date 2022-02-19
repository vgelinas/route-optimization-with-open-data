#!/bin/bash

# Activate environment variables
source /home/ubuntu/route-optimization-with-open-data/.venv/bin/activate 

if [[ -z $1 ]];
then 
	/home/ubuntu/route-optimization-with-open-data/.venv/bin/python3 /home/ubuntu/route-optimization-with-open-data/data_pipeline/run_pipeline.py -sc 
else
	# Pass along optional args. 
	/home/ubuntu/route-optimization-with-open-data/.venv/bin/python3 /home/ubuntu/route-optimization-with-open-data/data_pipeline/run_pipeline.py -sc $@
fi


