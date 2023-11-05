#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pyarrow.parquet as pq
import pandas as pd
import seaborn as sns
import argparse
from sqlalchemy import create_engine

from prefect import flow, task
from prefect.tasks import task_input_hash
# In[4]:

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    parquet_name = params.parquet_name


    #reading data and creating pandas data frame 

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    

    trips = pq.read_table(parquet_name)
    trips = trips.to_pandas()


    trips.tpep_pickup_datetime = pd.to_datetime(trips.tpep_pickup_datetime)
    trips.tpep_dropoff_datetime = pd.to_datetime(trips.tpep_dropoff_datetime)
    # In[21]:
    engine = create_engine('postgresql://root:root@localhost:5432/my_taxi')
    engine.connect()

    trips.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')
    trips.to_sql(name = table_name, con=engine, if_exists='append')

    # In[ ]:




if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help='an integer for the accumulator')
    parser.add_argument('--password', help='an integer for the accumulator')
    parser.add_argument('--host', help='an integer for the accumulator')
    parser.add_argument('--port', help='an integer for the accumulator')
    parser.add_argument('--db', help='an integer for the accumulator')
    parser.add_argument('--table_name', help='an integer for the accumulator')
    parser.add_argument('--url', help='an integer for the accumulator')
    parser.add_argument('--parquet_name', help='an integer for the accumulator')

    args = parser.parse_args()

    main(args)

