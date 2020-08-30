import os, datetime
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')

from google.cloud import bigquery

_PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT')
_DATASET_ID_EQUITY = 'daily_market_data_equity'
_TABLE_ID_DAILY = 'daily'
_FULL_TABLE_ID = '{p}.{d}.{t}'.format(p=_PROJECT_ID, d=_DATASET_ID_EQUITY, t=_TABLE_ID_DAILY)
_WRITE_QUEUE_SIZE_THRESHOLD = 4000
_POLYGON_API_KEY = os.environ['API_KEY_POLYGON']

from shutil import copyfile
#

'''
recent_date = datetime.date(2020, 8, 11)
oldest_date = datetime.date(2005, 1, 1)

date = oldest_date
prev_date = None
while True:
    if date == oldest_date or date.month != prev_date.month or (date.day <= 3 and date.weekday() == 0) or (date.month == 1 and (date.day <= 4 and date.weekday() < 5)):
        date_1st = date.replace(date.year, date.month, 1)
        src = 'data/daily/daily_{dt}.csv'.format(dt=str(date))
        dst = 'data/monthly/monthly_{dt}.csv'.format(dt=str(date_1st))
        print('from {s} to {d}'.format(s=src, d=dst))

        with open(dst, 'w') as of:
            for line in open(src):
                columns = line.strip().split(',')
                columns[0] = str(date_1st)
                of.write(','.join(columns)+'\n')
        #copyfile(src, dst)

    prev_date = date
    date += datetime.timedelta(days=1)
    if date > recent_date:
        break
#'''

#'''
with open('data/monthly/agg/monthly.csv', 'w') as of:
    of.write('date,symbol,open,high,low,close,volume,volume_weighted_price\n')
    for i, f in enumerate(sorted(list(os.listdir('data/monthly')))):
        print(i, f)
        if '.csv' not in f: continue
        for line in open('data/monthly/' + f, 'r'):
            splits = line.split(',')
            if len(splits) < 8:
                continue
            vol = float(splits[6])
            if vol == 0 or splits[-1] == '\n':
                continue
            symbol = splits[1]
            if symbol == 'symbol':
                continue
            if '.' in symbol:
                #print('. in the symbol {s}'.format(s=symbol))
                continue
            c, vw = float(splits[5]), float(splits[7])
            if abs((c - vw) / vw) > 0.03 and c > 1000:
                print('symbol {s} has invalid close vs. vw gap c: {c}, vw: {vw}'.format(s=symbol, c=c, vw=vw))
                continue
            of.write(line)
#'''
