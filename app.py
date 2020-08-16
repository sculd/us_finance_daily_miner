import os, time, datetime, logging

logging.basicConfig(level=logging.DEBUG)

from flask import Flask, request
import daily_sp500

# make sure these libraries don't log debug statement which can contain
# sensitive information
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

app = Flask(__name__)

@app.route('/dailysnp500', methods=['GET'])
def run():
    date = datetime.date.today() - datetime.timedelta(days=1)
    date = request.args.get('date') if request.args.get('date') else date
    table_id = request.args.get('table_id') if request.args.get('table_id') else daily_sp500.TABLE_ID_DAILY
    logging.info('date: {date}, table_id: {table_id}'.format(date=str(date), table_id=table_id))
    daily_sp500.export_daily_aggregate(str(date), table_id=table_id)
    return 'done'

@app.route('/hello', methods=['GET'])
def hello():
    return 'hello world'

if __name__ == '__main__':
    # Used when running locally only. When deploying to Cloud Run,
    # a webserver process such as Gunicorn will serve the app.
    app.run(host='localhost', port=8081, debug=True)
