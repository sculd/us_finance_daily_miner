import os, time, datetime, logging
import pytz
logging.basicConfig(level=logging.DEBUG)

from flask import Flask, request
import daily_stock
import outlier_analysis
import outlier_analysis_email

# make sure these libraries don't log debug statement which can contain
# sensitive information
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

app = Flask(__name__)

@app.route('/dailysnp500', methods=['GET'])
def handle_dailysnp500():
    pst = pytz.timezone('America/Los_Angeles')
    date = date = datetime.datetime.now(pst).date() - datetime.timedelta(days=1)
    if request.args.get('date'):
        date = datetime.datetime.strptime(request.args.get('date'), "%Y-%m-%d").date()
    snp500_table_id = request.args.get('snp500_table_id') if request.args.get('snp500_table_id') else daily_stock.TABLE_ID_DAILY_SNP500
    daily_stock.export_daily_aggregate_snp500(str(date), table_id=snp500_table_id)
    daily_stock.export_daily_aggregate(str(date))
    daily_stock.export_first_day_of_month(str(date))
    return 'done'

@app.route('/simfin', methods=['GET'])
def handle_simfin():
    pst = pytz.timezone('America/Los_Angeles')
    date = datetime.datetime.now(pst).date() - datetime.timedelta(days=1)
    if request.args.get('date'):
        date = datetime.datetime.strptime(request.args.get('date'), "%Y-%m-%d").date()
    table_id_simfin = request.args.get('table_id_simfin') if request.args.get('table_id_simfin') else daily_stock.TABLE_ID_DAILY_SIMFIN
    daily_stock.export_simfin(str(date), table_id=table_id_simfin)
    return 'done'

@app.route('/outlier_analysis', methods=['GET'])
def handle_outlier_analysis():
    outlier_analysis_email.send_email_report()
    return 'done'

@app.route('/hello', methods=['GET'])
def hello():
    return 'hello world'

if __name__ == '__main__':
    # Used when running locally only. When deploying to Cloud Run,
    # a webserver process such as Gunicorn will serve the app.
    app.run(host='localhost', port=8081, debug=True)
