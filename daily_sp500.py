import os, datetime
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')

import csv
import finnhub
from polygon import RESTClient
from google.cloud.bigquery.table import Row

from google.cloud import bigquery

_PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT')
_DATASET_ID_EQUITY = 'daily_market_data_equity'
TABLE_ID_DAILY = 'daily_snp500'
_TABLE_ID_DAILY_TEMP = 'temp'
_WRITE_QUEUE_SIZE_THRESHOLD = 4000
_POLYGON_API_KEY = os.environ['API_KEY_POLYGON']
_FINNHUB_API_KEY = os.environ['API_KEY_FINNHUB']

_bigquery_client = None
_polygon_client = RESTClient(_POLYGON_API_KEY)
_finnhub_client = finnhub.Client(api_key=_FINNHUB_API_KEY)

def get_full_table_id(table_id):
    return '{p}.{d}.{t}'.format(p=_PROJECT_ID, d=_DATASET_ID_EQUITY, t=table_id)

def get_big_query_client():
  global _bigquery_client
  if _bigquery_client is None:
      project_id = os.getenv('GOOGLE_CLOUD_PROJECT')
      _bigquery_client = bigquery.Client(project = project_id)
  return _bigquery_client

def _get_snp500_constituents():
    r = _finnhub_client.indices_const(symbol = "^GSPC")
    return set(r['constituents'])

def _get_daily_aggregate_results(date_str):
    resp = _polygon_client.stocks_equities_grouped_daily("us", "stocks", date_str)
    results = resp.results
    snp500_constituents = _get_snp500_constituents()
    return [r for r in results if r['T'] in snp500_constituents]

def _write_rows(rows, table_id):
    if not rows:
        return
    i = 0
    bq_client = get_big_query_client()
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

def export_daily_aggregate(date_str, table_id=TABLE_ID_DAILY):
    results = _get_daily_aggregate_results(date_str)
    _export_results(results, table_id)

