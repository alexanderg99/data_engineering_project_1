import argparse
from time import time
import pandas as pd

from sqlalchemy import create_engine

parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

parser.add_argument('user', help='an integer for the accumulator')
parser.add_argument('pass', help='an integer for the accumulator')
parser.add_argument('host', help='an integer for the accumulator')
parser.add_argument('post', help='an integer for the accumulator')
parser.add_argument('db', help='an integer for the accumulator')
parser.add_argument('table-name', help='an integer for the accumulator')
parser.add_argument('url', help='an integer for the accumulator')


args = parser.parse_args()