#!/usr/bin/env python
# coding: utf-8

# In[2]:


import snowflake.connector
import pandas as pd
import argparse

# In[4]:

def main(params):
    user = params.user
    password = params.password
    account = params.account
    warehouse = params.warehouse
    table_name = params.table_name
    database = params.database
    schema = params.schema
    path = params.path

    con = snowflake.connector.connect(
    user=user,
    password=password,
    account=account,
    warehouse=warehouse,
    database=database,
    schema=schema
    )

    cur = con.cursor()

    command = f'''
    COPY INTO {table_name} FROM @{path}
    FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1)
    '''
    cur.execute(command)




if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help='an integer for the accumulator')
    parser.add_argument('--password', help='an integer for the accumulator')
    parser.add_argument('--account', help='an integer for the accumulator')
    parser.add_argument('--port', help='an integer for the accumulator')
    parser.add_argument('--warehouse', help='an integer for the accumulator')
    parser.add_argument('--table_name', help='an integer for the accumulator')
    parser.add_argument('--database', help='an integer for the accumulator')
    parser.add_argument('--schema', help='an integer for the accumulator')
    parser.add_argument('--path', help='an integer for the accumulator')

    args = parser.parse_args()

    main(args)

