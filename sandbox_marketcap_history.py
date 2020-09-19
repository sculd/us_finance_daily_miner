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
    FROM `trading-290017.core_us_fundametals_data.core_us_fundamentals` 
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

'''
prev_date = None
with open('data/SHARADAR_SF1_smallcap_or_larger.csv', 'w') as of:
    of.write('ticker,dimension,calendardate,datekey,reportperiod,lastupdated,accoci,assets,assetsavg,assetsc,assetsnc,assetturnover,bvps,capex,cashneq,cashnequsd,cor,consolinc,currentratio,de,debt,debtc,debtnc,debtusd,deferredrev,depamor,deposits,divyield,dps,ebit,ebitda,ebitdamargin,ebitdausd,ebitusd,ebt,eps,epsdil,epsusd,equity,equityavg,equityusd,ev,evebit,evebitda,fcf,fcfps,fxusd,gp,grossmargin,intangibles,intexp,invcap,invcapavg,inventory,investments,investmentsc,investmentsnc,liabilities,liabilitiesc,liabilitiesnc,marketcap,ncf,ncfbus,ncfcommon,ncfdebt,ncfdiv,ncff,ncfi,ncfinv,ncfo,ncfx,netinc,netinccmn,netinccmnusd,netincdis,netincnci,netmargin,opex,opinc,payables,payoutratio,pb,pe,pe1,ppnenet,prefdivis,price,ps,ps1,receivables,retearn,revenue,revenueusd,rnd,roa,roe,roic,ros,sbcomp,sgna,sharefactor,sharesbas,shareswa,shareswadil,sps,tangibles,taxassets,taxexp,taxliabilities,tbvps,workingcapital\n')
    for line in open('data/SHARADAR_SF1.csv'):
        columns = line.split(',')
        if columns[60] == 'marketcap':
            continue
        if columns[60] == '':
            continue
        market_cap_m = float(columns[60]) / 1000000
        if market_cap_m < 600:
            continue
        of.write(line)
'''

'''
with open('data/universe_monthly_stocks.csv', 'w') as of:
    of.write('date,symbol\n')
    for line in open('data/monthly/agg/monthly.csv'):
        columns = line.split(',')
        of.write(','.join(columns[:2])+'\n')
#'''

'''
with open('data/universe_SF1_monthly.csv', 'w') as of:
    of.write('symbol,date\n')
    for line in open('data/universe_SF1.csv'):
        symbol, date_str = line.split(',')
        if symbol == 'ticker':
            continue
        date = datetime.datetime.strptime(date_str.strip(), '%Y-%m-%d').date()
        for i in range(3):
            new_month = (date.month + 1 + i) % 12
            if new_month == 0:
                new_month = 12
            new_yar = date.year + (1 if date.month + 1 + i > 12 else 0)
            of.write(','.join([symbol, str(date.replace(year=new_yar, month=new_month, day=1))]) + '\n')
#'''

'''
monthly_symbols_SF1 = {}
for line in open('data/universe_SF1_monthly.csv'):
    symbol, date_str = line.strip().split(',')
    if date_str not in monthly_symbols_SF1:
        monthly_symbols_SF1[date_str] = set()
    monthly_symbols_SF1[date_str].add(symbol)

with open('data/universe_combined.csv', 'w') as of:
    of.write('date,symbol\n')
    for line in open('data/universe_monthly_stocks.csv'):
        date_str, symbol = line.strip().split(',')
        if date_str not in monthly_symbols_SF1:
            continue
        if symbol not in monthly_symbols_SF1[date_str]:
            continue
        of.write(','.join([date_str, symbol]) + '\n')
'''

#for line in open('data/universe_monthly_stocks.csv'):

'''
monthly_market_caps = {}

import csv
with open("data/marketcap.csv", newline='') as csvfile:
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

