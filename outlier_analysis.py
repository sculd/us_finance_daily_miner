import os, datetime, logging
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')

from google.cloud import bigquery
from google.cloud import bigquery_storage

from collections import defaultdict
import numpy as np, pandas as pd
from sklearn import linear_model
from sklearn.metrics import mean_squared_error, r2_score

_PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT')
_DATASET_ID_EQUITY = 'daily_market_data_equity'
_TABLE_ID_DAILY = 'daily'
_FULL_TABLE_ID = '{p}.{d}.{t}'.format(p=_PROJECT_ID, d=_DATASET_ID_EQUITY, t=_TABLE_ID_DAILY)
_WRITE_QUEUE_SIZE_THRESHOLD = 4000
_POLYGON_API_KEY = os.environ['API_KEY_POLYGON']
_FINNHUB_API_KEY = os.environ['API_KEY_FINNHUB']

_bigquery_client = bigquery.Client(project = os.getenv('GOOGLE_CLOUD_PROJECT'))
_bqstorage_client = bigquery_storage.BigQueryReadClient()

from polygon import RESTClient
_polygon_client = RESTClient(_POLYGON_API_KEY)
_regr = linear_model.LinearRegression()

RTR_DAYS = [1, 5, 20, 60]
REVERSE_RTR_DAYS = [20, 40, 60]
MOMENTUM_SCORE_DAYS = [20, 40, 60]
REVERSE_MOMENTUM_DAYS = [20, 40, 60]

_QUERY = """
    SELECT *
    FROM `trading-290017.daily_market_data_equity.daily_snp500` 
    WHERE TRUE
    AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 150 DAY)
    ORDER BY date ASC, symbol
"""

_QUERY_SIMFIN = """
    SELECT date, ticker as symbol, close
    FROM `trading-290017.daily_market_data_equity.daily_simfin`
    WHERE TRUE
    AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 150 DAY)
    ORDER BY date ASC, symbol
"""

def read():
  query_job = _bigquery_client.query(_QUERY_SIMFIN).result()
  df = query_job.to_dataframe(bqstorage_client=_bqstorage_client)
  df = df.set_index(['date', 'symbol']).sort_index()
  return df

def get_rtr_df(df):
    for i in RTR_DAYS:
        print('rtr{i} days'.format(i=i))
        df['rtr{i}m'.format(i=i)] = df.groupby(level=1).diff(i).close / df.groupby(level=1).shift(i).close

    df_rtr = df.dropna()
    return df_rtr

def _get_reverse_rtr_(s):
    l = len(s)
    n = l
    rev_rtr = 0
    def local_rtr(head, tail, anchor):
        if tail < head:
            return 0
        return (s[tail] - s[head]) / s[anchor]

    # the reverse initial move should account for at least half
    for i in (int(n * 2 / 3),):
        t_rev_rtr = -1 * local_rtr(l-n, l-n+i, l-n) + 1 * local_rtr(l-n+i, l-1, l-n)
        if abs(t_rev_rtr) > abs(rev_rtr):
            rev_rtr = t_rev_rtr
    return rev_rtr

def get_reverse_rtr_df(df):
    for i in REVERSE_RTR_DAYS:
        print('reverse rtr{i} days'.format(i=i))
        dfr = df.groupby(level=1)['close'].rolling(i, min_periods=1).apply(_get_reverse_rtr_)
        df['reverse_rtr{i}m'.format(i=i)] = dfr.droplevel(0, axis="index")

    df_reverse_rtr = df.dropna()
    return df_reverse_rtr

def _get_r2_score(y, days):
    '''
    regression score, the higher the more linear the momentum.
    '''
    global _regr
    l = len(y)
    if days // 2 > l:
        return 0

    y = y[-days:]
    x = np.array([i for i in range(len(y))]).reshape(-1, 1)
    y = np.array(y).reshape(-1, 1)
    _regr.fit(x, y)
    y_pred = _regr.predict(x)
    return r2_score(y, y_pred)

def _get_return(y, days):
    l = len(y)
    if l == 0:
        print('length is zero')
        return 0
    head = max(0, l - days)
    return (y[-1] - y[head]) / y[head]

def _get_momentum_score(y, days):
    ret =  _get_return(y, days) * _get_r2_score(y, days)
    if type(ret) is np.ndarray:
        return ret[0]
    return ret

def get_momentum_df(df):
    # m score
    m_scores_per_symbol = defaultdict(list)
    symbols = df.close.dropna().index.levels[1]
    for symbol in symbols:
        for days in MOMENTUM_SCORE_DAYS:
            try:
                m_scores_per_symbol[symbol].append(_get_momentum_score(df.close.dropna().xs(symbol, level=1).values, days))
            except KeyError as e:
                print(e)

    recent_date = df.index.get_level_values(0)[-1]
    df_recent = df.xs(recent_date, level=0)
    df_mscores = pd.DataFrame.from_dict(m_scores_per_symbol, orient='index', columns=['m_score{d}'.format(d=m_day) for m_day in MOMENTUM_SCORE_DAYS])
    df_mscores = df_mscores.set_index(df_mscores.index.rename('symbol')).join(df_recent).dropna()
    return df_mscores

def _get_reverse_momentum_score(y, days):
    anochor = int(days * 2 / 3)
    head1 = max(0, len(y) - days)
    head2 = max(0, len(y) - days + anochor)
    return -1 * _get_momentum_score(y[head1:head2], anochor) + _get_momentum_score(y[head2:], days - anochor)

def get_reverse_momentum_df(df):
    # m score
    m_scores_per_symbol = defaultdict(list)
    symbols = df.close.dropna().index.levels[1]
    for symbol in symbols:
        for days in MOMENTUM_SCORE_DAYS:
            try:
                m_scores_per_symbol[symbol].append(_get_reverse_momentum_score(df.close.dropna().xs(symbol, level=1).values, days))
            except KeyError as e:
                print(e)

    recent_date = df.index.get_level_values(0)[-1]
    df_recent = df.xs(recent_date, level=0)
    df_mscores = pd.DataFrame.from_dict(m_scores_per_symbol, orient='index', columns=['m_score{d}'.format(d=m_day) for m_day in MOMENTUM_SCORE_DAYS])
    df_mscores = df_mscores.set_index(df_mscores.index.rename('symbol')).join(df_recent).dropna()
    return df_mscores

if __name__ == '__main__':
    s = pd.Series([10, 10, 10, 18, 15, 10])
    print(_get_reverse_rtr_(s))