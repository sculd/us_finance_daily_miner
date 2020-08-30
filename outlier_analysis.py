import os, datetime, logging
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')

from google.cloud import bigquery
from google.cloud import bigquery_storage_v1beta1

_PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT')
_DATASET_ID_EQUITY = 'daily_market_data_equity'
_TABLE_ID_DAILY = 'daily'
_FULL_TABLE_ID = '{p}.{d}.{t}'.format(p=_PROJECT_ID, d=_DATASET_ID_EQUITY, t=_TABLE_ID_DAILY)
_WRITE_QUEUE_SIZE_THRESHOLD = 4000
_POLYGON_API_KEY = os.environ['API_KEY_POLYGON']
_FINNHUB_API_KEY = os.environ['API_KEY_FINNHUB']

_bigquery_client = bigquery.Client(project = os.getenv('GOOGLE_CLOUD_PROJECT'))
_bqstorage_client = bigquery_storage_v1beta1.BigQueryStorageClient()

from polygon import RESTClient
_polygon_client = RESTClient(_POLYGON_API_KEY)

_QUERY = """
    SELECT *
    FROM `alpaca-trading-239601.daily_market_data_equity.daily_snp500` 
    WHERE TRUE
    AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 100 DAY)
    ORDER BY date ASC, symbol
"""

_QUERY_SIMFIN = """
    SELECT date, ticker as symbol, close
    FROM `alpaca-trading-239601.daily_market_data_equity.daily_simfin_2020`
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
    subject = "Daily Stock Analysis"
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
