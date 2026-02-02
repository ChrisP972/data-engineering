#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import click
from sqlalchemy import create_engine
from tqdm.auto import tqdm

datatypes = {
            "VendorID": "Int64",
            "passenger_count": "Int64",
            "trip_distance": "float64",
            "RatecodeID": "Int64",
            "store_and_fwd_flag": "string",
            "PULocationID": "Int64",
            "DOLocationID": "Int64",
            "payment_type": "Int64",
            "fare_amount": "float64",
            "extra": "float64",
            "mta_tax": "float64",
            "tip_amount": "float64",
            "tolls_amount": "float64",
            "improvement_surcharge": "float64",
            "total_amount": "float64",
            "congestion_surcharge": "float64"
        }

parse_date_columns = [
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime"
        ]   

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostegreSQL password')
@click.option('--pg-host', default='localhost', help='PostegreSQL host')
@click.option('--pg-port', default=5432, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')
@click.option('--year', default=2021, help='Target year')
@click.option('--month', default=1, help='Target year')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table, year, month):

    # Ingest NYC taxi data into PostgreSQL database
    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
    url = f'{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz'

        # Database connection parameters
    engine = create_engine(
        f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}'
    )

    chunksize = 100000

    df_iter = pd.read_csv(
        url, 
        dtype = datatypes,
        parse_dates = parse_date_columns,
        iterator = True,
        chunksize = chunksize,
    )

    first = True

    for df_chunk in tqdm(df_iter):
        if first:
            df_chunk.head(0).to_sql(
                name=target_table, 
                con=engine, 
                if_exists='replace'
            )
            
            print("\nTable Created")        
            first = False

        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists='append'
        )
            
        print("\nInserted:", len(df_chunk))

if __name__ == '__main__':
    run()