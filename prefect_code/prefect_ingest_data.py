from prefect import flow,task
import pandas as pd
from prefect_gcp.cloud_storage import GcsBucket
import argparse
import pyarrow.parquet as pq
import pyarrow as pa
from pathlib import Path


@task
def extract_data_from_url(url: str) -> pd.DataFrame:

    df = pd.read_parquet(url)
    return df 

@task(log_prints=True)
def data_processing(df: pd.DataFrame) -> pd.DataFrame:

    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df



@task()
def write_local(df: pd.DataFrame, filename) -> Path:
    path = Path(f"data/{filename}.parquet")
    df.to_parquet(path, compression="gzip")
    return path



@task
def write_to_gcs_bucket(path):
    #load stored gcp cloud storate bucket
    #how to initialize the gcp thing
    gcs_block = GcsBucket.load("taxi-bucket")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return 


@flow
def to_gcs(params):

    url = params.url
    filename = params.filename
    df = extract_data_from_url(url)
    cleaned_df = data_processing(df)
    p = write_local(cleaned_df, filename)
    write_to_gcs_bucket(p)

    



    return
    


if __name__ ==  "__main__":


    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    parser.add_argument('--filename', help='filename', default='parquet_data')
    
    parser.add_argument('--url', help='url of the data', default='https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet')
    args = parser.parse_args()
    to_gcs(args)
