import os, datetime
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')

from google.cloud import bigquery

_PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT')
_DATASET_ID_EQUITY = 'daily_market_data_equity'
_TABLE_ID_DAILY = 'daily'
_FULL_TABLE_ID = '{p}.{d}.{t}'.format(p=_PROJECT_ID, d=_DATASET_ID_EQUITY, t=_TABLE_ID_DAILY)
_WRITE_QUEUE_SIZE_THRESHOLD = 4000
_POLYGON_API_KEY = os.environ['API_KEY_POLYGON']
_FINNHUB_API_KEY = os.environ['API_KEY_FINNHUB']


import finnhub


finnhub_client = finnhub.Client(api_key=_FINNHUB_API_KEY)
print(finnhub_client.indices_const(symbol = "^GSPC"))


recent_date = datetime.date(2004, 12, 31)
oldest_date = datetime.date(2001, 1, 1)

#'''
import csv
date = oldest_date
prev_date = None
while True:
    if date > recent_date:
        break
    print(date)
    date_1st_day = date.replace(day = 1)
    date_1st_day_str = str(date_1st_day)
    if not os.path.exists("data/monthly/monthly_{dt}.csv".format(dt=date_1st_day_str)):
        if date == oldest_date or date.month != prev_date.month or (date.day <= 3 and date.weekday() == 0) or (
                date.month == 1 and (date.day <= 4 and date.weekday() < 5)):
            with open("data/monthly/monthly_{dt}.csv".format(dt=date_1st_day_str), 'w') as of:
                for line in open("data/daily/daily_{dt}.csv".format(dt=date_1st_day_str), newline=''):
                    line_symbol = line.split(',')[1]
                    of.write(line)

    prev_date = date
    date += datetime.timedelta(days=1)
#'''
