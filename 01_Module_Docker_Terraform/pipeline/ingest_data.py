import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click

# Specify Data types
dtype = {
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

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]



# Ingest Data into Progres
@click.command()
# @click.option('--year', default=2021, type=int, help='Year of data to ingest')
# @click.option('--month', default=1, type=int, help='Month of data to ingest')
@click.option('--pg-user', default='root', help='PostgreSQL username')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for reading data')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')

def run(year, month, pg_user, pg_pass, pg_host, pg_port, pg_db, chunksize, target_table):
    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
    url = f'{prefix}yellow_tripdata_{year}-{month:02d}.csv.gz'
    target_table = f'{target_table}_{year}_{month:02d}'
    # Create database connection
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')
    
    # Create an iterator to read each chunk from the url 
    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize
    )

    # Iterate over chunks
    first = True
    for df_chunk in tqdm(df_iter):
        # Create table schema (no data)
        if first:
            df_chunk.head(0).to_sql(
                name=target_table,
                con=engine,
                if_exists='replace'
            )
            first=False # once it's already run as first, change status first to False to continue to next chunk
            print('Table created')
        
        # Continue to next chunk
        df_chunk.to_sql(name=target_table, 
                                    con=engine,
                                    if_exists='append'
                                    )
        print(f'Inserted: {len(df_chunk)}')


# Execute
if __name__ == '__main__':
    run()



