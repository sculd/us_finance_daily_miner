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

_QUERY = """
    SELECT *
    FROM `trading-290017.daily_market_data_equity.daily_snp500` 
    WHERE TRUE
    AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 100 DAY)
    ORDER BY date ASC, symbol
"""

_QUERY_SIMFIN = """
    SELECT date, ticker as symbol, close
    FROM `trading-290017.daily_market_data_equity.daily_simfin`
    WHERE TRUE
    AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 100 DAY)
    ORDER BY date ASC, symbol
"""

def _read():
  query_job = _bigquery_client.query(_QUERY_SIMFIN).result()
  return query_job.to_dataframe(bqstorage_client=_bqstorage_client)

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


regr = linear_model.LinearRegression()

def _get_r2_score(y, days):
    global regr
    l = len(y)
    if days // 2 > l:
        return 0

    y = y[-days:]
    x = np.array([i for i in range(len(y))]).reshape(-1, 1)
    y = np.array(y).reshape(-1, 1)
    regr.fit(x, y)
    y_pred = regr.predict(x)
    return r2_score(y, y_pred)

def _get_return(y, days):
    l = len(y)
    head = max(0, l - days)
    return (y[-1] - y[head]) / y[head]

def get_momentum_score(y, days):
    ret =  _get_return(y, days) * _get_r2_score(y, days)
    if type(ret) is np.ndarray:
        return ret[0]
    return ret

symbols_to_graph = []

def get_top_gainer_html(df_momentums, recent_date, day):
    global symbols_to_graph
    display_day = day if day == 1 else int((day / 5) * 7)
    html_str = ''
    html_str += '<div style="width: 20%;float:left">\n'
    html_str += '<p>Top Gainer Over {d} Days</p>\n'.format(d=display_day)
    column = 'rtr{d}m'.format(d=day)
    html_str += ((df_momentums[df_momentums.close > 20].xs(str(recent_date), level=0).sort_values(column, ascending=False).head()[['close', column]].rename(columns={column: 'return over {d} days'.format(d=display_day)}) * 1).round(3).astype(str) + '').to_html()
    html_str += '\n</div>\n'

    for symbol in list(df_momentums[df_momentums.close > 20].xs(str(recent_date), level=0).sort_values(column, ascending=False).head().index):
        if symbol not in symbols_to_graph:
            symbols_to_graph.append(symbol)

    return html_str


def get_top_loser_html(df_momentums, recent_date, day):
    global symbols_to_graph
    display_day = day if day == 1 else int((day / 5) * 7)
    html_str = ''
    html_str += '<div style="width: 20%;float:left">\n'
    html_str += '<p>Top Loser Over {d} Days</p>'.format(d=display_day)
    column = 'rtr{d}m'.format(d=day)
    html_str += ((df_momentums[df_momentums.close > 20].xs(str(recent_date), level=0).sort_values(column, ascending=True).head()[['close', column]].rename(columns={column: 'return over {d} days'.format(d=display_day)}) * 1).round(3).astype(str) + '').to_html()
    html_str += '\n</div>\n'

    for symbol in list(df_momentums[df_momentums.close > 20].xs(str(recent_date), level=0).sort_values(column, ascending=True).head().index):
        if symbol not in symbols_to_graph:
            symbols_to_graph.append(symbol)

    return html_str

def get_top_bottom_mscores_html(df_mscores, day, top=True):
    global symbols_to_graph
    display_day = day if day == 1 else int((day / 5) * 7)
    html_str = ''
    html_str += '<div style="width: 20%;float:left">\n'
    html_str += '<p>{top} Over {d} Days</p>\n'.format(top='Top Gainer' if top else 'Bottom Loser', d=display_day)
    column = 'm_score{d}'.format(d=day)
    df_select = df_mscores[df_mscores.close > 20].sort_values('m_score{d}'.format(d=day), ascending=not top).head()[['close', column]].rename(columns={column: 'momentum score over {d} days'.format(d=display_day)})
    html_str += ((df_select * 1).round(3).astype(str) + '').to_html()
    html_str += '\n</div>\n'

    for symbol in list(df_select.index):
        if symbol not in symbols_to_graph:
            symbols_to_graph.append(symbol)
    return html_str

def get_symbol_info_html(symbol):
    html_str = ''
    html_str += '<p>{s}</p>'.format(s=symbol)
    try:
        resp = _polygon_client.reference_ticker_details(symbol)
        html_str += '<p>{s}, {i}</p>'.format(s=resp.sector if resp.sector else '', i=resp.industry if resp.industry else '')
        html_str += '<p>{d}</p>'.format(d=resp.description if resp.description else '')
    except Exception as ex:
        print(ex)
    return html_str

def get_chart_img_html_tag(symbol, recent_date):
    def _get_daily_history(symbol, start_date_str, end_date_str):
        '''
        returns labels and data
        '''
        resp = _polygon_client.stocks_equities_aggregates(symbol, 1, 'day', start_date_str, end_date_str)
        results = resp.results
        if results is None:
            return [], []
        return [str(datetime.datetime.utcfromtimestamp(int(r['t'] / 1000)).date()) for r in results], [str(r['c']) for r in results]

    html_str = ''
    for d_days in (7, 30, 120,):
        start_date_str = str(recent_date - datetime.timedelta(days=d_days))
        end_date_str = str(recent_date + datetime.timedelta(days=1))

        labels, data = _get_daily_history(symbol, start_date_str, end_date_str)
        html_str += '<div style="width: 30%;float:left">\n'
        html_str += """<img src="https://quickchart.io/chart?c={type:'line',data:{labels:[%s],datasets:[{label:'%s',data:[%s], fill:false,borderColor:'blue'}]}}"  width="100%%" height="auto" />""" % (
            ','.join(map(lambda l: "'{l}'".format(l=l), labels)), symbol, ','.join(data))
        html_str += '\n</div>\n'
    return html_str

def add_graph_html(recent_date, symbols):
    html_str = ''
    for symbol in symbols:
        html_str += get_symbol_info_html(symbol)
        html_str += get_chart_img_html_tag(symbol, recent_date)
        html_str += '<br clear="all" />'
    return html_str

def get_report_html():
    global symbols_to_graph
    symbols_to_graph = []
    df = _read()
    df = df.set_index(['date', 'symbol']).sort_index()

    days = [1, 5, 20, 60]
    for i in days:
        print('rtr{i}m'.format(i=i))
        df['rtr{i}m'.format(i=i)] = df.groupby(level=1).diff(i).close / df.groupby(level=1).shift(i).close

    df_momentums = df.dropna()
    df_momentums = df_momentums[['close'] + ['rtr{i}m'.format(i=i) for i in days]].round(3)

    recent_date = df_momentums.index.get_level_values(0)[-1].to_pydatetime().date()

    html_str = ''
    for d in days:
        html_str += get_top_gainer_html(df_momentums, recent_date, d)
    html_str += '<br clear="all" />'

    for d in days:
        html_str += get_top_loser_html(df_momentums, recent_date, d)
    html_str += '<br clear="all" />'

    # m score
    m_scores_per_symbol = defaultdict(list)
    symbols = df.close.dropna().index.levels[1]
    m_days = [20, 60]
    for symbol in symbols:
        for i in m_days:
            try:
                m_scores_per_symbol[symbol].append(get_momentum_score(df.close.dropna().xs(symbol, level=1).values, i))
            except KeyError as e:
                print(e)

    df_recent = df.xs(str(recent_date), level=0)
    df_mscores = pd.DataFrame.from_dict(m_scores_per_symbol, orient='index', columns=['m_score{d}'.format(d=m_day) for m_day in m_days])
    df_mscores = df_mscores.set_index(df_mscores.index.rename('symbol')).join(df_recent).dropna()

    m_days = [20, 60]
    for d in m_days:
        html_str += get_top_bottom_mscores_html(df_mscores, d, top=True)
    html_str += '<br clear="all" />'

    for d in m_days:
        html_str += get_top_bottom_mscores_html(df_mscores, d, top=False)
    html_str += '<br clear="all" />'

    html_str += add_graph_html(recent_date, symbols_to_graph)

    return html_str

import sendgrid
_sg = sendgrid.SendGridAPIClient(api_key=os.environ.get('SENDGRID_API_KEY'))
from sendgrid.helpers.mail import *
import ssl
#ssl._create_default_https_context = ssl._create_unverified_context
def send_email_report():
    from_email = Email("sculd3@gmail.com")
    to_email = To("sculd3@gmail.com")
    subject = "Daily Stock Analysis For " + datetime.date.today().strftime("%Y-%m-%d")
    html_str = get_report_html()
    content = Content("text/html", html_str)
    mail = Mail(from_email, to_email, subject, content)
    response = _sg.client.mail.send.post(request_body=mail.get())
    logging.info(response.status_code)
    logging.info(response.body)
    logging.info(response.headers)

if __name__ == '__main__':
    html_str = get_report_html()
    with open('sandbox_html.html', 'w') as of:
        of.write(html_str)
