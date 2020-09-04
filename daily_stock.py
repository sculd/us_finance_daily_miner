import os, datetime, logging
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')

import csv
import numpy as np
import finnhub
from polygon import RESTClient
from google.cloud.bigquery.table import Row
from google.cloud import bigquery
import simfin as sf

sf.set_api_key(os.getenv('API_KEY_SIMFIN'))

# Set the local directory where data-files are stored.
# The directory will be created if it does not already exist.
sf.set_data_dir('~/simfin_data/')

_PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT')
_DATASET_ID_EQUITY_DAILY = 'daily_market_data_equity'
_DATASET_ID_EQUITY_MONTHLY = 'monthly_market_data_equity'
TABLE_ID_DAILY_SNP500 = 'daily_snp500'
TABLE_ID_DAILY = 'daily'
TABLE_ID_DAILY_SIMFIN = 'daily_simfin'
TABLE_ID_MONTHLY = 'monthly'
_TABLE_ID_DAILY_TEMP = 'temp'
_TABLE_ID_DAILY_SIMFIN_TEMP = 'simfin_temp'
_WRITE_QUEUE_SIZE_THRESHOLD = 4000
_POLYGON_API_KEY = os.environ['API_KEY_POLYGON']
_FINNHUB_API_KEY = os.environ['API_KEY_FINNHUB']

_bigquery_client = None
_polygon_client = RESTClient(_POLYGON_API_KEY)
_finnhub_client = finnhub.Client(api_key=_FINNHUB_API_KEY)

def get_full_table_id(dataset_id, table_id):
    return '{p}.{d}.{t}'.format(p=_PROJECT_ID, d=dataset_id, t=table_id)

def get_big_query_client():
  global _bigquery_client
  if _bigquery_client is None:
      project_id = os.getenv('GOOGLE_CLOUD_PROJECT')
      _bigquery_client = bigquery.Client(project = project_id)
  return _bigquery_client

def _get_snp500_constituents():
    r = _finnhub_client.indices_const(symbol = "^GSPC")
    return set(r['constituents'])

def _get_daily_polygon_aggregate_results(date_str):
    resp = _polygon_client.stocks_equities_grouped_daily("us", "stocks", date_str)
    results = resp.results
    return results

def _write_rows(bq_rows, dataset_id, table_id):
    if not bq_rows:
        return
    i = 0
    bq_client = get_big_query_client()
    while True:
        bq_client.insert_rows(bq_client.get_table(get_full_table_id(dataset_id, table_id)), bq_rows[i:i + _WRITE_QUEUE_SIZE_THRESHOLD])
        i += _WRITE_QUEUE_SIZE_THRESHOLD
        if i >= len(bq_rows):
            break

def _export_polygon_results(results, dataset_id, table_id):
    def _result_to_bq_row(result):
        vw = result['vw'] if 'vw' in result else None
        return Row(
            (datetime.date.fromtimestamp(int(result['t'] / 1000.0)), result['T'].encode("ascii", "ignore").decode(), result['o'], result['h'], result['l'], result['c'], result['v'], vw),
            {c: i for i, c in enumerate(['date', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'volume_weighted_price'])}
        )

    bq_rows = [_result_to_bq_row(result) for result in results]
    _write_rows(bq_rows, dataset_id, table_id)

def export_daily_aggregate_snp500(date_str, table_id=TABLE_ID_DAILY_SNP500):
    polygon_results = _get_daily_polygon_aggregate_results(date_str)
    snp500_constituents = _get_snp500_constituents()
    results_snp500 = [r for r in polygon_results if r['T'] in snp500_constituents]
    logging.info('daily export snp500 symbols, date: {date}, table_id: {table_id}'.format(date=date_str, table_id=table_id))
    _export_polygon_results(results_snp500, _DATASET_ID_EQUITY_DAILY, table_id)

def export_daily_aggregate(date_str, table_id=TABLE_ID_DAILY):
    results = _get_daily_polygon_aggregate_results(date_str)
    date = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
    table_id_y = '{t}_{y}'.format(t=table_id, y=date.year)
    logging.info('daily export full symbols, date: {date}, table_id: {table_id}'.format(date=date_str, table_id=table_id_y))
    _export_polygon_results(results, _DATASET_ID_EQUITY_DAILY, table_id_y)

def export_first_day_of_month(date_str, table_id=TABLE_ID_MONTHLY):
    results = _get_daily_polygon_aggregate_results(date_str)
    date = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
    if date.weekday() != 0 and date.day > 2:
        logging.info('skipping monthly write as it is not the first day of the month, date: {date}'.format(date=date_str))
        return
    logging.info('monthly export full symbols, date: {date}, table_id: {table_id}'.format(date=date_str, table_id=table_id))
    _export_polygon_results(results, _DATASET_ID_EQUITY_MONTHLY, table_id)

def _export_simfin_df(df, dataset_id, table_id):
    def _nan_to_none(v):
        if np.isnan(v):
            return None
        return v

    def _df_row_to_bq_row(index, row):
        return Row(
            (index[0], index[1].date(), _nan_to_none(row['SimFinId']), _nan_to_none(row['Open']), _nan_to_none(row['High']), _nan_to_none(row['Low']), _nan_to_none(row['Close']), _nan_to_none(row['Adj. Close']), _nan_to_none(row['Dividend']), _nan_to_none(row['Volume']), _nan_to_none(row['Shares Outstanding'])),
            {c: i for i, c in enumerate(['Ticker', 'Date', 'SimFinId', 'Open', 'High', 'Low', 'Close', 'Adj__Close', 'Dividend', 'Volume', 'Shares_Outstanding'])}
        )

    bq_rows = [_df_row_to_bq_row(index, row) for index, row in df.iterrows()]
    _write_rows(bq_rows, dataset_id, table_id)

def export_simfin(date_str, table_id=_TABLE_ID_DAILY_SIMFIN_TEMP):
    logging.info('daily export simfin data, date: {date}, table_id: {table_id}'.format(date=date_str, table_id=table_id))
    df = sf.load_shareprices(variant='latest', market='us', refresh_days=0)
    #df = sf.load_shareprices(variant='daily', market='us')
    try:
        df_date = df.xs(date_str, level=1, drop_level=False)
        if len(df_date) == 0:
            logging.info('can not find any data for the given date {d}'.format(d=date_str))
        _export_simfin_df(df_date, _DATASET_ID_EQUITY_DAILY, table_id)
    except Exception as ex:
        logging.error(ex)

if __name__ == '__main__':
    export_simfin('2020-09-01', table_id=TABLE_ID_DAILY_SIMFIN) # _TABLE_ID_DAILY_SIMFIN_TEMP
