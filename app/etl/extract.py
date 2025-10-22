import os
import json
import pandas as pd
import certifi
import urllib3



BASE_DIR= os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data', 'raw')

csv_path = os.path.join(DATA_DIR, "h9gi-nx95.csv")
parquet_path = os.path.join(DATA_DIR, "yellow_tripdata_2025-01.parquet")

df_csv= pd.read_csv(csv_path)
df_csv.head()

df_parquet= pd.read_parquet(parquet_path)
df_parquet.head()

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

print(source_data_from_api(api_end_point))