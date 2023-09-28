
# script to push prepped csv into neo4j db
import os, sys
import numpy as np
import pandas as pd
import glob
import datetime

now = datetime.datetime.now()
# Format the date to 'day-month-year'
DATE = now.strftime('%d%m%y')

sys.path.append('../db_util')
from neo4j_service import Neo4jService

# need to run neo4j docker first 
# bash neo4j_docker.sh


# connect to the neo4j db
neo4j_pw = 'neo4jpwd'
# set up connection to neo4j instance.
uri = "bolt://localhost:7687"
user = "neo4j"
password = neo4j_pw
neo4j_service = Neo4jService(uri, user, password)

# mounted volume location
mount_root = '../mount/neo4j'

# processed csv location
job_date = 'example'
csv_files = glob.glob(f'../eth_tracker_output/{job_date}/*/*.csv')

# set uniqueness rule for nodes (addresses), need to be run just once. 
# neo4j_service.set_unique_rule() 

for csv_file in csv_files:
    parsed = csv_file.split('/')
    
    #convert the path into an internal one
    internal_csv_path = '/'.join(parsed[2:])
    # create/merge all relations 
    neo4j_service.batch_merge_trace_filter_csv(internal_csv_path, batch_size=1000)


########################################################################
# purge
neo4j_service.purge()


