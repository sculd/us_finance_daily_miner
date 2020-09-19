import os, datetime
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')

import csv
import finnhub
from polygon import RESTClient
from google.cloud.bigquery.table import Row

from google.cloud import bigquery

_PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT')
_POLYGON_API_KEY = os.environ['API_KEY_POLYGON']

_bigquery_client = bigquery.Client(project=os.getenv('GOOGLE_CLOUD_PROJECT'))
_polygon_client = RESTClient(_POLYGON_API_KEY)

_QUERY = """
    WITH MARKETCAP AS (
    SELECT date, Ticker, ROUND(AVG(market_cap) / 1000000, 0) as avg_market_cap
    FROM `trading-290017.simfin.us_derived_shareprices_monthly` 
    WHERE TRUE
    AND MARKET_CAP > 0
    AND MARKET_CAP > 600 * 1000000
    AND date >= "2011-01-01"
    GROUP BY date, Ticker
    ORDER BY date ASC
    )
    
    SELECT BASE.date, BASE.Ticker as symbol
    FROM MARKETCAP JOIN `trading-290017.simfin.us_derived_shareprices_monthly` AS BASE ON TRUE
      AND MARKETCAP.date = BASE.date
      AND MARKETCAP.Ticker = BASE.Ticker
    ORDER BY date, symbol
"""

def fetch():
  query_job = _bigquery_client.query(_QUERY).result()
  rows = list(query_job)
  return rows

def _bq_rows_as_csv_file(csv_file_name, rows):
    def _bq_row_to_csv_line(row):
        return '{date},{symbol}\n'.format(
            date=row[0],
            symbol=row[1]
        )

    with open(csv_file_name, 'w') as of:
        of.write('date,symbol\n')
        for row in rows:
            of.write(_bq_row_to_csv_line(row))


if __name__ == '__main__':
    rows = fetch()
    _bq_rows_as_csv_file('data/universe.csv', rows)
