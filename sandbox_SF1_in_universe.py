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


monthly_universe = {}
for line in open("data/universe_combined.csv"):
    date_str, symbol = line.strip().split(',')
    if date_str == 'date':
        continue
    if date_str not in monthly_universe:
        monthly_universe[date_str] = set()
    monthly_universe[date_str].add(symbol)


def next_month_first_date_str(date_str, month_offset):
    date = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
    new_month = (date.month + month_offset) % 12
    if new_month == 0:
        new_month = 12
    new_year = date.year + (1 if date.month + month_offset > 12 else 0)
    return str(date.replace(year=new_year, month=new_month, day=1))

'''
prev_date = None
with open('data/SHARADAR_SF1_combined_universe.csv', 'w') as of:
    of.write('ticker,dimension,calendardate,datekey,reportperiod,lastupdated,accoci,assets,assetsavg,assetsc,assetsnc,assetturnover,bvps,capex,cashneq,cashnequsd,cor,consolinc,currentratio,de,debt,debtc,debtnc,debtusd,deferredrev,depamor,deposits,divyield,dps,ebit,ebitda,ebitdamargin,ebitdausd,ebitusd,ebt,eps,epsdil,epsusd,equity,equityavg,equityusd,ev,evebit,evebitda,fcf,fcfps,fxusd,gp,grossmargin,intangibles,intexp,invcap,invcapavg,inventory,investments,investmentsc,investmentsnc,liabilities,liabilitiesc,liabilitiesnc,marketcap,ncf,ncfbus,ncfcommon,ncfdebt,ncfdiv,ncff,ncfi,ncfinv,ncfo,ncfx,netinc,netinccmn,netinccmnusd,netincdis,netincnci,netmargin,opex,opinc,payables,payoutratio,pb,pe,pe1,ppnenet,prefdivis,price,ps,ps1,receivables,retearn,revenue,revenueusd,rnd,roa,roe,roic,ros,sbcomp,sgna,sharefactor,sharesbas,shareswa,shareswadil,sps,tangibles,taxassets,taxexp,taxliabilities,tbvps,workingcapital\n')
    for line in open('data/SHARADAR_SF1_smallcap_or_larger.csv'):
        columns = line.split(',')
        symbol = columns[0]
        if symbol == 'ticker':
            continue
        date_str = columns[2]
        next_date_str = next_month_first_date_str(date_str, 1)
        if next_date_str not in monthly_universe:
            #print(next_date_str, 'not in universe index')
            continue
        if symbol not in monthly_universe[next_date_str]:
            continue
        of.write(line)
#'''

'''
with open('data/universe_monthly_stocks.csv', 'w') as of:
    of.write('date,symbol\n')
    for line in open('data/monthly/agg/monthly.csv'):
        columns = line.split(',')
        of.write(','.join(columns[:2])+'\n')
'''

#'''
with open('data/SHARADAR_SF1_monthly_combined_universe_MRQ.csv', 'w') as of:
    #of.write('ticker,dimension,calendardate,' + 'datekey,reportperiod,lastupdated,' + 'accoci,assets,assetsavg,assetsc,assetsnc,assetturnover,bvps,capex,cashneq,cashnequsd,cor,consolinc,currentratio,de,debt,debtc,debtnc,debtusd,deferredrev,depamor,deposits,divyield,dps,ebit,ebitda,ebitdamargin,ebitdausd,ebitusd,ebt,eps,epsdil,epsusd,equity,equityavg,equityusd,ev,evebit,evebitda,fcf,fcfps,fxusd,gp,grossmargin,intangibles,intexp,invcap,invcapavg,inventory,investments,investmentsc,investmentsnc,liabilities,liabilitiesc,liabilitiesnc,marketcap,ncf,ncfbus,ncfcommon,ncfdebt,ncfdiv,ncff,ncfi,ncfinv,ncfo,ncfx,netinc,netinccmn,netinccmnusd,netincdis,netincnci,netmargin,opex,opinc,payables,payoutratio,pb,pe,pe1,ppnenet,prefdivis,price,ps,ps1,receivables,retearn,revenue,revenueusd,rnd,roa,roe,roic,ros,sbcomp,sgna,sharefactor,sharesbas,shareswa,shareswadil,sps,tangibles,taxassets,taxexp,taxliabilities,tbvps,workingcapital\n')
    of.write('ticker,dimension,calendardate,' + 'accoci,assets,assetsavg,assetsc,assetsnc,assetturnover,bvps,capex,cashneq,cashnequsd,cor,consolinc,currentratio,de,debt,debtc,debtnc,debtusd,deferredrev,depamor,deposits,divyield,dps,ebit,ebitda,ebitdamargin,ebitdausd,ebitusd,ebt,eps,epsdil,epsusd,equity,equityavg,equityusd,ev,evebit,evebitda,fcf,fcfps,fxusd,gp,grossmargin,intangibles,intexp,invcap,invcapavg,inventory,investments,investmentsc,investmentsnc,liabilities,liabilitiesc,liabilitiesnc,marketcap,ncf,ncfbus,ncfcommon,ncfdebt,ncfdiv,ncff,ncfi,ncfinv,ncfo,ncfx,netinc,netinccmn,netinccmnusd,netincdis,netincnci,netmargin,opex,opinc,payables,payoutratio,pb,pe,pe1,ppnenet,prefdivis,price,ps,ps1,receivables,retearn,revenue,revenueusd,rnd,roa,roe,roic,ros,sbcomp,sgna,sharefactor,sharesbas,shareswa,shareswadil,sps,tangibles,taxassets,taxexp,taxliabilities,tbvps,workingcapital\n')
    for line in open('data/SHARADAR_SF1_combined_universe_MRQ.csv'):
        columns = line.strip().split(',')
        date_str = columns[0]
        symbol = columns[1]
        if symbol == 'ticker' or symbol == 'symbol':
            continue
        for i in range(3):
            columns[0] = next_month_first_date_str(date_str.strip(), 1 + i)
            of_line = ','.join(columns) + '\n'
            of.write(of_line)
#'''


