import os, time, datetime, logging

logging.basicConfig(level=logging.DEBUG)

from flask import Flask, request
import daily_stock

# make sure these libraries don't log debug statement which can contain
# sensitive information
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

app = Flask(__name__)

@app.route('/dailysnp500', methods=['GET'])
def run():
    date = datetime.date.today() - datetime.timedelta(days=1)
    date = request.args.get('date') if request.args.get('date') else date
    snp500_table_id = request.args.get('table_id') if request.args.get('snp500_table_id') else daily_stock.TABLE_ID_DAILY
    daily_stock.export_daily_aggregate_snp500(str(date), table_id=snp500_table_id)
    daily_stock.export_daily_aggregate(str(date))
    return 'done'

@app.route('/hello', methods=['GET'])
def hello():
    return 'hello world'

if __name__ == '__main__':
    # Used when running locally only. When deploying to Cloud Run,
    # a webserver process such as Gunicorn will serve the app.
    app.run(host='localhost', port=8081, debug=True)
