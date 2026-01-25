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
@click.option('--table-name', default='green_taxi_trips', help='Table name to insert data into')
@click.option('--parquet-file-path', required=True, help='Path to parquet file')
@click.option('--batch-size', default=100_000, type=int, help='Batch size for parquet iteration')

def ingest_data(pg_user, pg_password, pg_host, pg_port, pg_db, table_name, parquet_file_path, batch_size):
    """Ingest green taxi trip data from parquet file to PostgreSQL database."""
    
    # Create database connection string
    engine = create_engine(f'postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}')
    
    # Load parquet file
    parquet_file = pq.ParquetFile(parquet_file_path)
    
    first = True
    
    for pq_batch in tqdm(parquet_file.iter_batches(batch_size=batch_size)):
        df_chunk = pq_batch.to_pandas()

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







