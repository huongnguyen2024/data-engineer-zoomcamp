import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click
import pyarrow.parquet as pq


@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL username')
@click.option('--pg-password', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default='5432', help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--table-name', default='zones', help='Table name to insert data into')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for reading data')

def ingest_data(pg_user, pg_password, pg_host, pg_port, pg_db, table_name, chunksize):
    """Ingest zones data from csv file to PostgreSQL database."""
    
    # Create database connection 
    engine = create_engine(f'postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}')
    url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv'

    # Create an iterator to read each chunk from the url 
    df_iter = pd.read_csv(
        url,
        iterator=True,
        chunksize=chunksize
    )
    
    first = True
    for df_chunk in tqdm(df_iter):

        if first:
            # Create table schema (NO data)
            df_chunk.head(0).to_sql(
                name=table_name,
                con=engine,
                if_exists='replace'
            )
            first = False
            print('Table created')

        # Insert chunk
        df_chunk.to_sql(
            name=table_name,
            con=engine,
            if_exists='append'
        )
        print(f'Inserted: {len(df_chunk)} rows')


if __name__ == '__main__':
    ingest_data()







