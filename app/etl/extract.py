import os
import json
import pandas as pd
import certifi
import urllib3



BASE_DIR= os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data', 'raw')

api_end_point= "https://jsonplaceholder.typicode.com/posts"

def source_data_from_api(url):
    global pd_api
    try:
        http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())
        api_response = http.request('GET', url)

        if api_response.status == 200:
            api_response_json = json.loads(api_response.data.decode('utf-8'))
            pd_api = pd.json_normalize(api_response_json)
            print('Successfully fetched data')
        else:
            pd_api = pd.DataFrame()
            print(f"Request failed with status {api_response.status}")
    except Exception as e:
        print(e)
    return pd_api

# print(source_data_from_api(api_end_point))


csv_path = os.path.join(DATA_DIR, "h9gi-nx95.csv")

def source_data_from_csv(csv_file_path):
    try:
        df_csv = pd.read_csv(csv_file_path)
    except Exception as e:
        df_csv = pd.DataFrame()
        print(e)
    return df_csv

# print(source_data_from_csv(csv_path))


parquet_path = os.path.join(DATA_DIR, "yellow_tripdata_2025-01.parquet")


df_parquet= pd.read_parquet(parquet_path)

def source_data_from_parquet(parquet_file_path):
    try:
        df_parquet = pd.read_parquet(parquet_file_path)
    except Exception as e:
        df_parquet = pd.DataFrame()
        print(e)
    return df_parquet

source_data_from_parquet(df_parquet)