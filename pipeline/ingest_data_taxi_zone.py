#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import click
from sqlalchemy import create_engine

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostegreSQL password')
@click.option('--pg-host', default='localhost', help='PostegreSQL host')
@click.option('--pg-port', default=5432, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--target-table', default='taxi_zone', help='Target table name')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table):

    # Ingest NYC taxi data into PostgreSQL database
    url = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv'

    # Database connection parameters
    engine = create_engine(
        f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}'
    )

    df = pd.read_csv(url)

    df.to_sql(
        name=target_table,
        con=engine,
        if_exists='replace',
        )

                
if __name__ == '__main__':
    run()