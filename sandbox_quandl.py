import os, quandl

_API_KEY_QUANDL = os.getenv('API_KEY_QUANDL')
quandl.ApiConfig.api_key = _API_KEY_QUANDL

# r = quandl.get_table('SHARADAR/EVENTS', date='2016-04-18')
r = quandl.get_table('SHARADAR/SF1', ticker='AAPL')

for c in r.columns:
    print(c)

print(r)

