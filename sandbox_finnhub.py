import os, datetime
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')

import csv
import pprint
import finnhub
from polygon import RESTClient
import requests
import websocket
from google.cloud import bigquery
from newsapi import NewsApiClient

_PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT')
_POLYGON_API_KEY = os.environ['API_KEY_POLYGON']
_FINNHUB_API_KEY = os.environ['API_KEY_FINNHUB']
_NEWSAPI_API_KEY = os.environ['API_KEY_NEWSAPI']

_bigquery_client = bigquery.Client(project=os.getenv('GOOGLE_CLOUD_PROJECT'))
_polygon_client = RESTClient(_POLYGON_API_KEY)
_finnhub_client = finnhub.Client(api_key=_FINNHUB_API_KEY)



#pprint.pprint(_finnhub_client.company_news('TSLA', '2020-08-27', '2020-08-31'))


_newsapi_client = NewsApiClient(api_key=_NEWSAPI_API_KEY)

top_headlines = _newsapi_client.get_top_headlines(category='business', language='en', country='us')
pprint.pprint(top_headlines)

# /v2/everything
all_articles = _newsapi_client.get_everything(q='tesla', from_param='2020-08-27', to='2020-08-31', language='en', sort_by='publishedAt')
#pprint.pprint(all_articles)


import requests
#r = requests.get('https://finnhub.io/api/v1/news?category=general&token={token}'.format(token=_FINNHUB_API_KEY))
r = requests.get('https://finnhub.io/api/v1/company-news?symbol=AAPL&from=2020-08-27&to=2020-08-30&token={token}'.format(token=_FINNHUB_API_KEY))
pprint.pprint(r.json())

def on_message(ws, message):
    print(message)

def on_error(ws, error):
    print(error)

def on_close(ws):
    print("### closed ###")

def on_open(ws):
    ws.send('{"type":"subscribe-news","symbol":"AAPL"}')
    ws.send('{"type":"subscribe-news","symbol":"AMZN"}')
    ws.send('{"type":"subscribe-news","symbol":"MSFT"}')
    ws.send('{"type":"subscribe-news","symbol":"BYND"}')

if __name__ == "__main__":
    websocket.enableTrace(True)
    '''
    ws = websocket.WebSocketApp("wss://ws.finnhub.io?token={token}".format(token=_FINNHUB_API_KEY),
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()
    '''



