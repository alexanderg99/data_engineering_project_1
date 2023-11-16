from prefect import flow,task
import pandas as pd
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
import argparse
import pyarrow.parquet as pq
import pyarrow as pa
from pathlib import Path

# This is a script that takes some files from GCP, transforms it, then uploads
# it into bigquery. 

@task(retries=3)
def extract_from_gcs() -> Path:
    #Download trip data from GCS, the data lake.
    gcs_path = "data/parquet_data.parquet"
    gcs_block = GcsBucket.load("taxi-bucket")
    #the download function
    #https://prefecthq.github.io/prefect-gcp/cloud_storage/#prefect_gcp.cloud_storage.GcsBucket
    gcs_block.get_directory(from_path=gcs_path, local_path=f"./data/")
    return Path(f"./data/{gcs_path}")

@task()
def transform(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df['passenger_count'].fillna(0, inplace=True)
    return df

@task()
def write_bq(df: pd.DataFrame) -> None:

    gcp_credentials_block = GcpCredentials.load("gcp-creds")
    pid = "plenary-ellipse-403613"
    did = "yellow_taxi"

    df.to_gbq(
        
        project_id="plenary-ellipse-403613",
        destination_table=f"{pid}.{did}.yellow_taxi_rides",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    
    )
    

@flow()
def gcs_to_bq():
    #Main ETL flow to load data into BigQuery
    path = extract_from_gcs()
    df = transform(path)
    write_bq(df)


#to do: figure out the funciton to load and write the parquet file into the 
#bigquery thing. check the documentation. 



if __name__ == "__main__":
    gcs_to_bq()