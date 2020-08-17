import os, datetime
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')

import csv
import finnhub
from polygon import RESTClient
from google.cloud.bigquery.table import Row

from google.cloud import bigquery

_PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT')
_DATASET_ID_EQUITY = 'daily_market_data_equity'
TABLE_ID_DAILY = 'daily'
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

def export_daily_aggregate(date_str, table_id=_TABLE_ID_DAILY_TEMP):
    results = _get_daily_aggregate_results(date_str)
    _export_results(results, table_id)


#export_daily_aggregate('2015-08-17', table_id=_TABLE_ID_DAILY_TEMP)

#'''
#recent_date = datetime.date(2020, 8, 12)
recent_date = datetime.date(2017, 7, 20)
oldest_date = datetime.date(2005, 1, 1)

date = recent_date
while True:
    print('exporting for', date)
    date_str = str(date)

    export_daily_aggregate(date_str, table_id=get_daily_table_id(date.year))

    date -= datetime.timedelta(days=1)
    if date < oldest_date:
        break
#'''


def create_table(year):
    schema = [
        bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("symbol", "STRING"),
        bigquery.SchemaField("open", "FLOAT"),
        bigquery.SchemaField("high", "FLOAT"),
        bigquery.SchemaField("low", "FLOAT"),
        bigquery.SchemaField("close", "FLOAT"),
        bigquery.SchemaField("volume", "FLOAT"),
        bigquery.SchemaField("volume_weighted_price", "FLOAT"),
    ]

    full_table_id = get_full_table_id('{t}_{y}'.format(t=TABLE_ID_DAILY, y=year))
    table = bigquery.Table(full_table_id, schema=schema)
    table = _bigquery_client.create_table(table)  # Make an API request.
