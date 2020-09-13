import os, datetime, pytz
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
_polygon_client = RESTClient(_POLYGON_API_KEY)
_finnhub_client = finnhub.Client(api_key=_FINNHUB_API_KEY)

def get_daily_table_id(year):
    return '{t}_{y}'.format(t=TABLE_ID_DAILY, y=year)

def get_full_table_id(table_id):
    return '{p}.{d}.{t}'.format(p=_PROJECT_ID, d=_DATASET_ID_EQUITY, t=table_id)

def _get_minute_aggregate_results_one_batch(symbol, start_date, end_date_exclusive):
    print('querying for {s} from {f} to {t}'.format(s=symbol, f=str(start_date), t=str(end_date_exclusive)))
    resp = _polygon_client.stocks_equities_aggregates(symbol, 1, 'minute', str(start_date), str(end_date_exclusive))
    results = resp.results
    return results

def _get_minute_aggregate_results(symbol, start_date, end_date_exclusive):
    results = []
    batch_days = datetime.timedelta(days=10)
    d = start_date
    while d < end_date_exclusive:
        start_d = d
        end_d = d + batch_days
        if end_d > end_date_exclusive:
            end_d = end_date_exclusive
        results += _get_minute_aggregate_results_one_batch(symbol, start_d, end_d)
        d += batch_days
    return results

def _create_new_csv_with_header(csv_filename):
    with open(csv_filename, 'w') as o_file:
        o_file.write(','.join(['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume']) + '\n')

def _results_to_csv_tuples(symbol, results):
    def _is_within_market_hour(result):
        t = pytz.utc.localize(datetime.datetime.utcfromtimestamp(int(result['t'] / 1000.0)))
        t_value = t.hour * 60 + t.minute
        return t_value >= 13 * 60 + 30 and t_value <= 20 * 60

    def _result_to_tuple(result):
        t = pytz.utc.localize(datetime.datetime.utcfromtimestamp(int(result['t'] / 1000.0)))
        return (int(t.timestamp()), symbol, result['o'], result['h'], result['l'], result['c'], result['v'])

    tuples = [_result_to_tuple(result) for result in results if _is_within_market_hour(result)]
    return tuples

def _tuples_to_csv_file(tuples, csv_filename, is_append=True):
    open_mode = 'a' if is_append else 'w'
    with open(csv_filename, open_mode) as o_file:
        for tuple in tuples:
            o_file.write(','.join(map(lambda t: str(t), tuple)) + '\n')

def _results_to_csv(symbol, results, csv_filename):
    tuples = _results_to_csv_tuples(symbol, results)
    _tuples_to_csv_file(tuples, csv_filename)

def export_daily_aggregate(symbol, start_date, end_date, out_filename = None):
    out_filename = out_filename if out_filename else 'data/by_minute/{s}_{f}_{t}.csv'.format(s=symbol, f=str(start_date), t=str(end_date))
    _create_new_csv_with_header(out_filename)
    results = _get_minute_aggregate_results(symbol, start_date, end_date + datetime.timedelta(days=1))
    _results_to_csv(symbol, results, 'data/by_minute/{s}_{f}_{t}.csv'.format(s=symbol, f=str(start_date), t=str(end_date)))

def export_daily_aggregate_for_symbols(symbols, start_date, end_date, out_filename):
    _create_new_csv_with_header(out_filename)
    end_date_exclusive = end_date + datetime.timedelta(days=1)
    tuples = []
    for symbol in symbols:
        tuples += _results_to_csv_tuples(symbol, _get_minute_aggregate_results(symbol, start_date, end_date_exclusive))
    _tuples_to_csv_file(tuples, out_filename)

import util.symbols

# _get_minute_aggregate_results_one_batch('AAPL', datetime.date(2020, 6, 1), datetime.date(2020, 9, 2))

start_date, end_date = datetime.date(2020, 6, 1), datetime.date(2020, 9, 11)
#'''
print('hotel_resort')
export_daily_aggregate_for_symbols(util.symbols.get_symbols_hotel_resort(), start_date, end_date, 'data/by_minute/{s}_{f}_{t}.csv'.format(s='hotel_resort', f=str(start_date), t=str(end_date)))
print('airline')
export_daily_aggregate_for_symbols(util.symbols.get_symbols_airline(), start_date, end_date, 'data/by_minute/{s}_{f}_{t}.csv'.format(s='airline', f=str(start_date), t=str(end_date)))
print('oil_gas')
export_daily_aggregate_for_symbols(util.symbols.get_symbols_oil_gas(), start_date, end_date, 'data/by_minute/{s}_{f}_{t}.csv'.format(s='oil_gas', f=str(start_date), t=str(end_date)))
print('restaurant')
export_daily_aggregate_for_symbols(util.symbols.get_symbols_restaurant(), start_date, end_date, 'data/by_minute/{s}_{f}_{t}.csv'.format(s='restaurant', f=str(start_date), t=str(end_date)))
#'''


filenames = []
filenames.append('data/by_minute/{s}_{f}_{t}_noheader.csv'.format(s='hotel_resort', f=str(start_date), t=str(end_date)))
filenames.append('data/by_minute/{s}_{f}_{t}_noheader.csv'.format(s='airline', f=str(start_date), t=str(end_date)))
filenames.append('data/by_minute/{s}_{f}_{t}_noheader.csv'.format(s='oil_gas', f=str(start_date), t=str(end_date)))
filenames.append('data/by_minute/{s}_{f}_{t}_noheader.csv'.format(s='restaurant', f=str(start_date), t=str(end_date)))

with open('data/by_minute/pair_trading_{f}_{t}.csv'.format(f=str(start_date), t=str(end_date)), 'w') as o_f:
    for filename in filenames:
        for line in open(filename):
            o_f.write(line)



