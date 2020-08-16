import os, datetime
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')

_FINNHUB_API_KEY = os.environ['API_KEY_FINNHUB']

import finnhub

finnhub_client = finnhub.Client(api_key=_FINNHUB_API_KEY)
r = finnhub_client.indices_const(symbol = "^GSPC")
print(r['constituents'])


#'''
recent_date = datetime.date(2020, 8, 11)
oldest_date = datetime.date(2005, 1, 1)
constituents = {}
import csv
with open("data/daily/constituent/historical_components.csv", newline='') as csvfile:
    csv_reader = csv.reader(csvfile, delimiter=',', quotechar='"')
    for row in csv_reader:
        date_str = row[0]
        if date_str == 'date':
            continue
        constituents[date_str] = set(row[1].split(','))

date = oldest_date
prev_date = date
while True:
    if str(date) not in constituents and str(prev_date) in constituents:
        constituents[str(date)] = constituents[str(prev_date)]
    prev_date = date
    date += datetime.timedelta(days=1)
    if date > recent_date:
        break
