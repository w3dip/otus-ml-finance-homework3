#import datetime
import pendulum
import os

#import requests
#from airflow.decorators import dag, task
#from airflow.providers.postgres.hooks.postgres import PostgresHook
#from airflow.providers.postgres.operators.postgres import PostgresOperator
#from airflow.models.taskinstance import TaskInstance
from airflow.models import Variable

import ccxt
import time
import pandas as pd
from datetime import datetime, timedelta

#start_date = task_instance.prev
#print(context)
exchange = ccxt.binance()
symbol = 'BTC/USD'
timeframe = '1h'

# prev_data_interval_end_success: pendulum.DateTime = Variable.get("prev_data_interval_end_success")
# print("prev_data_interval_end_success = ", prev_data_interval_end_success.to_iso8601_string())
# start_date = exchange.parse8601(prev_data_interval_end_success.to_iso8601_string())
#
# data_interval_start: pendulum.DateTime = Variable.get("data_interval_start")
# print("data_interval_start = ", data_interval_start.to_iso8601_string())
# end_date = exchange.parse8601(data_interval_start.to_iso8601_string())

#print("Context", kwargs)

# Variable to be created before the running of dag
#full_load_check = Variable.get('full_load_check')
#print('full_load_check : {0}'.format(full_load_check))

#prev_data_interval_end_success = None
prev_data_interval_end_success = pendulum.datetime(2024, 1, 1, tz="UTC")
#data_interval_end = pendulum.datetime(2024, 11, 17, tz="UTC")
data_interval_end = pendulum.datetime(2024, 4, 1, tz="UTC")

if prev_data_interval_end_success is None:
    start_date = pendulum.datetime(2024, 1, 1, tz="UTC").to_iso8601_string()
    print("start_date", start_date)
    start_date = exchange.parse8601(start_date)
    end_date = data_interval_end.to_iso8601_string()
    print("end_date", end_date)
    end_date = exchange.parse8601(end_date)
    #params = {}
else:
    start_date_dt = prev_data_interval_end_success
    end_date_dt = data_interval_end.subtract(hours=1)
    #diff = start_date_dt.diff(end_date_dt).in_days()

    # if diff > 200:
    #     start_date_dt = end_date_dt.subtract(days=200)

    start_date = exchange.parse8601(start_date_dt.to_iso8601_string())
    print("start_date", start_date_dt.to_iso8601_string())

    end_date = exchange.parse8601(end_date_dt.to_iso8601_string())
    print("end_date", end_date_dt.to_iso8601_string())

    #params = {'until': end_date}



# if full_load_check == '0':
#     print('First execution')
#
#     #print('Execution date : {0}'.format(kwargs.get('execution_date')))
#     #print('Actual start date : {0}'.format(kwargs.get('ds')))
#     #print('Previous successful execution date : {0}'.format(kwargs.get('prev_execution_date_success')))
#     #print('Calculated field : {0}'.format(datetime.strftime(datetime.now() - timedelta(days=365), '%Y-%m-%d')))
#     Variable.set('full_load_check', '1')
#     start_date = pendulum.now(tz="UTC").add(days=-7).to_iso8601_string()
#     #start_date = datetime.strftime(datetime.now() - timedelta(days=365), '%Y-%m-%d')
#     print("start_date", start_date)
#
#
#     end_date =
#     #end_date = datetime.strftime(kwargs.get('execution_date'), '%Y-%m-%d')
#     print("end_date", end_date)
# else:
#     print('After the first execution ..')
#     print('Execution date : {0}'.format(kwargs.get('execution_date')))
#     print('Actual start date : {0}'.format(kwargs.get('ds')))
#     print('Previous successful execution date : {0}'.format(kwargs.get('prev_execution_date_success')))
#     print('Calculated field : {0}'.format(kwargs.get('prev_execution_date_success')))
#     start_date = kwargs.get('prev_execution_date_success')
#     print("start_date", start_date)
#     #start_date = parse(str(start_date))
#     end_date = kwargs.get('execution_date')
#     #end_date = parse(str(end_date))
#     print("end_date", end_date)
#     #print('Type of start_date_check : {0}'.format(type(start_date)))
#     #start_date = datetime.strftime(start_date, '%Y-%m-%d')
#     #end_date = datetime.strftime(end_date, '%Y-%m-%d')

all_ohlcvs = []

while True:
    try:
        # print("loop start_date", exchange.iso8601(start_date))
        # end_date = params.get('until')
        # if end_date is not None:
        #     print("loop end_date", exchange.iso8601(end_date))
        #     if start_date > end_date:
        #         break

        #ohlcvs = exchange.fetch_ohlcv(symbol, timeframe, start_date, params=params, limit=500)
        ohlcvs = exchange.fetch_ohlcv(symbol, timeframe, start_date)
        #all_ohlcvs += ohlcvs

        # for row in ohlcvs:
        #     print("date", exchange.iso8601(row[0]))

        if len(ohlcvs):
            print('Fetched', len(ohlcvs), symbol, timeframe, 'candles from', exchange.iso8601(ohlcvs[0][0]))
            if ohlcvs[-1][0] > end_date:
                ohlcvs = filter(lambda x: x[0] <= end_date, ohlcvs)
                all_ohlcvs += ohlcvs
                break
            else:
                all_ohlcvs += ohlcvs

            start_date = ohlcvs[-1][0] + 1
            #params = {'until': ohlcvs[0][0]}

            sleep_interval = exchange.rateLimit / 1000
            print('Sleep for', sleep_interval)
            time.sleep(sleep_interval)
        else:
            break
    except Exception as e:
        print(type(e).__name__, str(e))
        break
# print('Fetched', len(all_ohlcvs), symbol, timeframe, 'candles in total')

df = pd.DataFrame(all_ohlcvs)
df.columns = ['date', 'open', 'high', 'low', 'close', 'volume']
df = df.sort_values(by='date')
df = df.drop_duplicates(subset='date').reset_index(drop=True)
df['date'] = pd.to_datetime(df['date'], unit='ms')
df = df.set_index('date')
dest_file = 'BTC_USD_1h.csv'
df.to_csv(dest_file)