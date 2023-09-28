
# script to push prepped csv into postgre db
import os, sys
import numpy as np
import pandas as pd
import glob
import datetime
import csv
now = datetime.datetime.now()
# Format the date to 'day-month-year'
DATE = now.strftime('%d%m%y')

sys.path.append('../db_util')
from pg_service import PGService

# intialize
# Database connection parameters

session_config = {}
session_config['DB_NAME'] = "postgres"
session_config['DB_USER'] = "postgres"
session_config['DB_PASS']= "pgpwd"
session_config['DB_HOST'] = "localhost"
session_config['DB_PORT'] = "5432"
pg_service = PGService(session_config)

# create table 
pg_service.cursor.execute("""
CREATE TABLE IF NOT EXISTS traces (
    from_address VARCHAR(255),
    to_address VARCHAR(255),
    block_number INTEGER,
    transaction_position INTEGER,
    subtraces INTEGER,
    trace_address JSONB,
    hierarchy INTEGER,
    decoded VARCHAR(255)
);
""")
                          
# delete table 
# pg_service.cursor.execute("DROP TABLE IF EXISTS transactions;")

# get all csv_files 
job_date = 'example'
csv_files = glob.glob(f'../eth_tracker_output/{job_date}/*/*.csv')

# Read the CSV file and insert its contents into the table in batches
for csv_file in csv_files:
    with open(csv_file, 'r') as file:
        reader = csv.DictReader(file)
        data = [(row['from'], row['to'], row['blockNumber'], row['transactionPosition'], row['subtraces'], row['traceAddress'], row['hierarchy'], row['decoded']) for row in reader]
        
        pg_service.cursor.executemany("""
        INSERT INTO traces (from_address, to_address, block_number, transaction_position, subtraces, trace_address, hierarchy, decoded)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """, data)



# Commit the transaction
pg_service.conn.commit()

# Close the cursor and connection
pg_service.cursor.close()
pg_service.conn.close()
