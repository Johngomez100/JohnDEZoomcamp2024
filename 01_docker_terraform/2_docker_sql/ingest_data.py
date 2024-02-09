#!/usr/bin/env python
# coding: utf-8

import os
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    
    # Define the output file name based on the URL
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    # Download the CSV file using wget
    os.system(f"wget -O {csv_name} {url}")

    # Connect to the PostgreSQL database
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Read the CSV file in chunks and insert into the database
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)
    
    # Convert datetime columns to datetime format
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    # Create an empty table in the database with the correct schema
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    # Insert the first chunk into the table
    df.to_sql(name=table_name, con=engine, if_exists='append')

    # Continue inserting remaining chunks into the table
    while True:
        try:
            t_start = time()
            df = next(df_iter)
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
            df.to_sql(name=table_name, con=engine, if_exists='append')
            t_end = time()
            print('Inserted another chunk, took %.3f seconds' % (t_end - t_start))
        except StopIteration:
            print("Finished ingesting data into the PostgreSQL database")
            break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to PostgreSQL')
    
    # Define script arguments
    parser.add_argument('--user', required=True, help='PostgreSQL user name')
    parser.add_argument('--password', required=True, help='PostgreSQL password')
    parser.add_argument('--host', required=True, help='PostgreSQL host address')
    parser.add_argument('--port', required=True, help='PostgreSQL port number')
    parser.add_argument('--db', required=True, help='PostgreSQL database name')
    parser.add_argument('--table_name', required=True, help='Name of the table in PostgreSQL')
    parser.add_argument('--url', required=True, help='URL of the CSV file')

    args = parser.parse_args()

    # Execute the main function with provided arguments
    main(args)
