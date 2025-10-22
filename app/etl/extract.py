import os
import json
import pandas as pd
import certifi
import urllib3
import sqlite3
import logging
import sys


BASE_DIR= os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data', 'raw')
sys.path.append(BASE_DIR)

from core.config import log_config

# Activate logging
log_config()
logger = logging.getLogger(__name__)

api_end_point= "https://jsonplaceholder.typicode.com/posts"

def source_data_from_api(url):
    global pd_api
    try:
        http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())
        api_response = http.request('GET', url)
        api_status = api_response.status
        if api_status == 200:
            logger.info(f'status code: {api_status} - OK , invoking data from {url} done successfully' )
            api_response_json = json.loads(api_response.data.decode('utf-8'))
            pd_api = pd.json_normalize(api_response_json)
            logger.info(f'status code : {api_status} - OK , {pd_api.shape[0]} rows extracted from {url}')
        else:
            pd_api = pd.DataFrame()
            logger.error(f'status code : {api_status} - ERROR , while invoking data from {url}')
            # print(f"Request failed with status {api_response.status}")
    except Exception as e:
        logger.exception(f'status code : {api_status} - Exception {e} encountered while reading data from {url}')
        df_api = pd.DataFrame()
        # print(e)
    return pd_api

# print(source_data_from_api(api_end_point))


csv_path = os.path.join(DATA_DIR, "h9gi-nx95.csv")

def source_data_from_csv(csv_file_path):
    try:
        df_csv = pd.read_csv(csv_file_path)
        logger.info(f'{csv_file_path} extracted : {df_csv.shape[0]} records read from the file')
    except Exception as e:
        logger.exception(f'{e} Exception encountered while reading data from {csv_file_path} ')
        df_csv = pd.DataFrame()
        # print(e)
    return df_csv

# print(source_data_from_csv(csv_path))


parquet_path = os.path.join(DATA_DIR, "yellow_tripdata_2025-01.parquet")


df_parquet= pd.read_parquet(parquet_path)

def source_data_from_parquet(parquet_file_path):
    try:
        df_parquet = pd.read_parquet(parquet_file_path)
        logger.info(f'{parquet_file_path} : extracted {df_parquet.shape[0]} records from the parguet file')
    except Exception as e:
        logger.exception( f'{parquet_file_path} : - exception {e} encountered while extracting the parguet file')
        df_parquet = pd.DataFrame()
        # print(e)
    return df_parquet

# source_data_from_parquet(df_parquet)
db_path = os.path.join(DATA_DIR, "movies.sqlite")

def source_data_from_table(db_name, table_name):
    try:
        with sqlite3.connect(db_name) as conn:

            tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", conn)
            print(f'list of tables in {db_name} :\n  {tables}')

            df_table = pd.read_sql(f"SELECT * from {table_name}", conn)
            logger.info(f'{db_name} read : {df_table.shape[0]} records from {table_name}')
    except Exception as e:
        logger.exception(f'{db_name} - Excepton {e} encountered while reading data from {tables}')
        df_table = pd.DataFrame()
        # print()
    return df_table

# print(source_data_from_table(db_path, "movies"))

webpage_url= "https://www.tgju.org/profile/geram18"
def source_data_from_html(html_url, matching_keywords):
    try:
        df_html = pd.read_html(html_url, match=matching_keywords)
        df_html = df_html[0]
        logger.info(f'{df_html.shape[0]} records from {html_url} : table {df_html} extracted')
    except Exception as e:
        logger.exception(f'Exception {e} encountered while reading data from {html_url}')
        df_html = pd.DataFrame()
        # print(e)
    return df_html

# www.tgju.org دریافت نرخ قیمت طلا از جدول مربوطه در وب سایت
# print(source_data_from_html(webpage_url, 'خصیصه'))


def extract_data_from_all_resource():
    csv_path = os.path.join(DATA_DIR, "h9gi-nx95.csv")
    parquet_path = os.path.join(DATA_DIR, "yellow_tripdata_2025-01.parquet")
    api_end_point = "https://jsonplaceholder.typicode.com/posts"
    db_name = os.path.join(DATA_DIR, "movies.sqlite")
    df_table = "movies"
    webpage_url = "https://www.tgju.org/profile/geram18"
    matching_word = "خصیصه"

    df_csv, df_parquet, df_api, df_table, df_webpage = (source_data_from_csv(csv_path),
                                                        source_data_from_parquet(parquet_path),
                                                        source_data_from_api(api_end_point),
                                                        source_data_from_table(db_name, df_table),
                                                        source_data_from_html(webpage_url,matching_word))

    return df_csv, df_parquet, df_api, df_table, df_webpage

extracted_data = extract_data_from_all_resource()
for data in extracted_data:
    print(data)