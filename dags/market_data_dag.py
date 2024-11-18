#import datetime
import pendulum
import os

#import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
#from airflow.providers.postgres.operators.postgres import PostgresOperator
#from airflow.models.taskinstance import TaskInstance
from airflow.models import Variable

import ccxt
import time
import pandas as pd
from datetime import datetime, timedelta

@dag(
    dag_id="process_crypto_data",
    #schedule_interval="*/2 * * * *",
    schedule_interval="@daily",
    start_date=pendulum.now(tz="UTC"),
    #end_date=pendulum.datetime(2024, 1, 2, tz="UTC"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
)
def process():
    # create_employees_table = PostgresOperator(
    #     task_id="create_employees_table",
    #     postgres_conn_id="tutorial_pg_conn",
    #     sql="""
    #         CREATE TABLE IF NOT EXISTS employees (
    #             "Serial Number" NUMERIC PRIMARY KEY,
    #             "Company Name" TEXT,
    #             "Employee Markme" TEXT,
    #             "Description" TEXT,
    #             "Leave" INTEGER
    #         );""",
    # )

    # create_employees_temp_table = PostgresOperator(
    #     task_id="create_employees_temp_table",
    #     postgres_conn_id="tutorial_pg_conn",
    #     sql="""
    #         DROP TABLE IF EXISTS employees_temp;
    #         CREATE TABLE employees_temp (
    #             "Serial Number" NUMERIC PRIMARY KEY,
    #             "Company Name" TEXT,
    #             "Employee Markme" TEXT,
    #             "Description" TEXT,
    #             "Leave" INTEGER
    #         );""",
    # )

    @task
    def get_data(**kwargs):
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

        data_interval_start: pendulum.DateTime = kwargs.get('data_interval_start')
        print('data_interval_start', data_interval_start.to_iso8601_string())

        data_interval_end: pendulum.DateTime = kwargs.get('data_interval_end')
        print('data_interval_end', data_interval_end.to_iso8601_string())

        prev_data_interval_start_success: pendulum.DateTime = kwargs.get('prev_data_interval_start_success')
        if prev_data_interval_start_success is not None:
            print('prev_data_interval_start_success', prev_data_interval_start_success.to_iso8601_string())
        else:
            print('prev_data_interval_start_success is None')

        prev_data_interval_end_success: pendulum.DateTime = kwargs.get('prev_data_interval_end_success')
        if prev_data_interval_end_success is not None:
            print('prev_data_interval_end_success', prev_data_interval_end_success.to_iso8601_string())
        else:
            print('prev_data_interval_end_success is None')

        prev_start_date_success: pendulum.DateTime = kwargs.get('prev_start_date_success')
        if prev_start_date_success is not None:
            print('prev_start_date_success=', prev_start_date_success.to_iso8601_string())
        else:
            print('prev_start_date_success is None')

        prev_end_date_success: pendulum.DateTime = kwargs.get('prev_end_date_success')
        if prev_end_date_success is not None:
            print('prev_end_date_success=', prev_end_date_success.to_iso8601_string())
        else:
            print('prev_end_date_success is None')

        if prev_data_interval_end_success is None:
            start_date = pendulum.datetime(2024, 1, 1, tz="UTC").to_iso8601_string()
            print("start_date", start_date)
            start_date = exchange.parse8601(start_date)
            end_date = data_interval_end.to_iso8601_string()
            print("end_date", end_date)
            end_date = exchange.parse8601(end_date)
        else:
            start_date = prev_data_interval_end_success.to_iso8601_string()
            print("start_date", start_date)
            start_date = exchange.parse8601(start_date)
            end_date = data_interval_end.to_iso8601_string()
            print("end_date", end_date)
            end_date = exchange.parse8601(end_date)

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
                ohlcvs = exchange.fetch_ohlcv(symbol, timeframe, start_date, params = {'until':end_date}, limit=200)
                all_ohlcvs += ohlcvs
                if len(ohlcvs):
                    print('Fetched', len(ohlcvs), symbol, timeframe, 'candles from', exchange.iso8601(ohlcvs[0][0]))
                    start_date = ohlcvs[-1][0] + 1
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
        dest_file = '~/BTC_USD_1h.csv'
        df.to_csv(dest_file)
        return dest_file

    @task
    def clean(source_file):
        df = pd.read_csv(source_file)
        df = df.set_index('date')
        dest_file = '~/BTC_USD_1h_cleaned.csv'
        df.to_csv(dest_file)
        return dest_file

    @task
    def add_features(source_file):
        df = pd.read_csv(source_file)
        df = df.set_index('date')
        dest_file = '~/BTC_USD_1h_with_new_features.csv'
        df.to_csv(dest_file)
        return dest_file

    @task
    def save_to_database(source_file):
        df = pd.read_csv(source_file)
        df = df.set_index('date')
        postgres_hook = PostgresHook(postgres_conn_id="postgres_data_storage")
        df.to_sql('btc_usd', postgres_hook.get_sqlalchemy_engine(), if_exists='append', chunksize=1000)
        return

        # NOTE: configure this as appropriate for your airflow environment
        # data_path = "/opt/airflow/dags/files/employees.csv"
        # os.makedirs(os.path.dirname(data_path), exist_ok=True)
        #
        # url = "https://raw.githubusercontent.com/apache/airflow/main/docs/apache-airflow/tutorial/pipeline_example.csv"
        #
        # response = requests.request("GET", url)
        #
        # with open(data_path, "w") as file:
        #     file.write(response.text)

        # postgres_hook = PostgresHook(postgres_conn_id="hw3_pg_conn")
        # conn = postgres_hook.get_conn()
        # cur = conn.cursor()
        # with open(data_path, "r") as file:
        #     cur.copy_expert(
        #         "COPY employees_temp FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
        #         file,
        #     )
        # conn.commit()

    # @task
    # def merge_data():
    #     query = """
    #         INSERT INTO employees
    #         SELECT *
    #         FROM (
    #             SELECT DISTINCT *
    #             FROM employees_temp
    #         ) t
    #         ON CONFLICT ("Serial Number") DO UPDATE
    #         SET
    #           "Employee Markme" = excluded."Employee Markme",
    #           "Description" = excluded."Description",
    #           "Leave" = excluded."Leave";
    #     """
    #     try:
    #         postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
    #         conn = postgres_hook.get_conn()
    #         cur = conn.cursor()
    #         cur.execute(query)
    #         conn.commit()
    #         return 0
    #     except Exception as e:
    #         return 1

    #[create_employees_table, create_employees_temp_table] >> get_data() >> merge_data()

    original_data = get_data()
    cleaned_data = clean(original_data)
    data_with_new_features = add_features(cleaned_data)
    save_to_database(data_with_new_features)

dag = process()