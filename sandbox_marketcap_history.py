import os, datetime
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')

import csv
import finnhub
from polygon import RESTClient
from google.cloud.bigquery.table import Row

from google.cloud import bigquery

_PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT')
_POLYGON_API_KEY = os.environ['API_KEY_POLYGON']
_FINNHUB_API_KEY = os.environ['API_KEY_FINNHUB']

_bigquery_client = bigquery.Client(project=os.getenv('GOOGLE_CLOUD_PROJECT'))
_polygon_client = RESTClient(_POLYGON_API_KEY)
_finnhub_client = finnhub.Client(api_key=_FINNHUB_API_KEY)

_QUERY = """
    SELECT ticker, calendardate , CAST(AVG(marketcap) / 1000000 AS INT64) AS marketcap
    FROM `alpaca-trading-239601.core_us_fundametals_data.core_us_fundamentals` 
    WHERE TRUE
    AND calendardate != "2020-03-31"
    AND marketcap IS NOT NULL
    GROUP BY ticker, calendardate
    ORDER BY calendardate ASC, ticker
"""

def read():
  query_job = _bigquery_client.query(_QUERY).result()
  rows = list(query_job)
  return rows

def _bq_rows_as_csv_file(csv_file_name, rows):
    def _bq_row_to_csv_line(row):
        return '{symbol},{date},{marketcap}\n'.format(
            symbol=row[0],
            date=row[1],
            marketcap=row[2]
        )

    with open(csv_file_name, 'w') as of:
        of.write('symbol,date,marketcap\n')
        for row in rows:
            of.write(_bq_row_to_csv_line(row))


if __name__ == '__main__':
    pass
    #rows = read()
    #_bq_rows_as_csv_file('marketcap.csv', rows)

prev_date = None
with open('us-derived-shareprices-monthly.csv', 'w') as of:
    of.write('Ticker,SimFinId,Date,Market-Cap,"Price to Earnings Ratio (quarterly)","Price to Earnings Ratio (ttm)","Price to Sales Ratio (quarterly)","Price to Sales Ratio (ttm)","Price to Book Value","Price to Free Cash Flow (quarterly)","Price to Free Cash Flow (ttm)","Enterprise Value",EV/EBITDA,EV/Sales,EV/FCF,"Book to Market Value","Operating Income/EV"\n')
    for line in open('us-derived-shareprices-daily.csv'):
        columns = line.split(';')
        date_str = columns[2]
        if date_str == 'Date':
            continue
        dt = datetime.datetime.strptime(date_str, '%Y-%m-%d')
        date = dt.date()
        if prev_date is not None and date.month != prev_date.month:
            date_1st = date.replace(date.year, date.month, 1)
            columns[2] = str(date_1st)
            of.write(','.join(columns))

        prev_date = date

'''
monthly_market_caps = {}

import csv
with open("marketcap.csv", newline='') as csvfile:
    csv_reader = csv.reader(csvfile, delimiter=',', quotechar='"')
    for row in csv_reader:
        date_str = row[1]
        if date_str == 'date':
            continue
        if date_str not in monthly_market_caps:
            monthly_market_caps[date_str] = {}
        monthly_market_caps[date_str][row[0]] = row[2]


recent_date = datetime.date(2020, 8, 1)
oldest_date = datetime.date(2005, 4, 1)

date = oldest_date
while True:
    print('for', date)
    a_day_before = date - datetime.timedelta(days=1)
    date_str = str(date)

    date += datetime.timedelta(days=1)
    if date > recent_date:
        break
#'''

