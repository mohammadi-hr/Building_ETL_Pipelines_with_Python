import http

import pandas as pd
import certifi
import urllib3
from urllib3 import request

df_csv= pd.read_csv("data/raw/h9gi-nx95.csv")
df_csv.head()

df_parquet= pd.read_parquet(
    "data/raw/yellow_tripdata_2025-01.parquet",
)
df_parquet.head()

url= 'https://data.cityofnewyork.us/resource/h9gi-nx95.json?$limit=500'
api_status= http.request('GET', url)
