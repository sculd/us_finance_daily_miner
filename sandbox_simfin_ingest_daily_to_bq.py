import os, datetime
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')

import csv
import finnhub
from polygon import RESTClient
from google.cloud.bigquery.table import Row

from google.cloud import bigquery

_PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT')
_DATASET_ID_EQUITY = 'daily_market_data_equity'
TABLE_ID_DAILY = 'daily_simfin'
_TABLE_ID_DAILY_TEMP = 'temp'
_WRITE_QUEUE_SIZE_THRESHOLD = 9000
_POLYGON_API_KEY = os.environ['API_KEY_POLYGON']
_FINNHUB_API_KEY = os.environ['API_KEY_FINNHUB']

_bigquery_client = bigquery.Client(project=os.getenv('GOOGLE_CLOUD_PROJECT'))
_polygon_client = RESTClient(_POLYGON_API_KEY)
_finnhub_client = finnhub.Client(api_key=_FINNHUB_API_KEY)

def get_daily_table_id(year):
    return '{t}_{y}'.format(t=TABLE_ID_DAILY, y=year)

def get_full_table_id(table_id):
    return '{p}.{d}.{t}'.format(p=_PROJECT_ID, d=_DATASET_ID_EQUITY, t=table_id)

def _get_daily_aggregate_results(date_str):
    resp = _polygon_client.stocks_equities_grouped_daily("us", "stocks", date_str)
    results = resp.results
    return results

def _write_rows(rows, table_id):
    if not rows:
        return
    i = 0
    bq_client = _bigquery_client
    while True:
        bq_client.insert_rows(bq_client.get_table(get_full_table_id(table_id)), rows[i:i + _WRITE_QUEUE_SIZE_THRESHOLD])
        i += _WRITE_QUEUE_SIZE_THRESHOLD
        if i >= len(rows):
            break

def _export_results(results, table_id):
    def _result_to_row(result):
        vw = result['vw'] if 'vw' in result else None
        return Row(
            (datetime.date.fromtimestamp(int(result['t'] / 1000.0)), result['T'].encode("ascii", "ignore").decode(), result['o'], result['h'], result['l'], result['c'], result['v'], vw),
            {c: i for i, c in enumerate(['date', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'volume_weighted_price'])}
        )

    rows = [_result_to_row(result) for result in results]
    _write_rows(rows, table_id)


def create_table(year):
    schema = [
        bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("symbol", "STRING"),
        bigquery.SchemaField("open", "FLOAT"),
        bigquery.SchemaField("high", "FLOAT"),
        bigquery.SchemaField("low", "FLOAT"),
        bigquery.SchemaField("close", "FLOAT"),
        bigquery.SchemaField("adj_close", "FLOAT"),
        bigquery.SchemaField("dividend", "FLOAT"),
        bigquery.SchemaField("volume", "FLOAT"),
        bigquery.SchemaField("shares_outstanding", "FLOAT"),
    ]

    full_table_id = get_full_table_id('{t}_{y}'.format(t=TABLE_ID_DAILY, y=year))
    table = bigquery.Table(full_table_id, schema=schema)
    table = _bigquery_client.create_table(table)  # Make an API request.

import simfin as sf

def ingest():
    sf.set_api_key(os.getenv('API_KEY_SIMFIN'))

    # Set the local directory where data-files are stored.
    # The directory will be created if it does not already exist.
    sf.set_data_dir('~/simfin_data/')

    df = sf.load_shareprices(variant='daily', market='us')

    for y in range(2010, 2021):
        df_y = df.xs(slice('{y}-01-01'.format(y=y), '{y}-12-31'.format(y=y)), level='Date', drop_level=False)
        df_y.to_csv('data/daily_simfin_{y}.csv'.format(y=y))


import requests
import pandas as pd

_SIMFIN_API_KEY = os.getenv('API_KEY_SIMFIN')

# list of tickers we want to get data for
tickers = ["AAPL", "NVDA", "WMT"]

# define the periods that we want to retrieve
periods = ["q1", "q2", "q3", "q4"]
year_start = 2012
year_end = 2020

# request url for all financial statements
request_url = 'https://simfin.com/api/v2/companies/prices'

# variable to store the names of the columns
columns = []
# variable to store our data
output = []

# with simfin+, we can retrieve all data in just one call; this is much faster than making individual requests
# define the parameters for the query
parameters = {
    "ticker": ",".join(tickers),
    "period": "quarters",
    "start": "2020-08-25",
    "end": "2020-09-01",
    "api-key": _SIMFIN_API_KEY}
# make the request
request = requests.get(request_url, parameters)


# convert response to json
all_data = request.json()

print(all_data)
