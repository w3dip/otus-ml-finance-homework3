import datetime
import pendulum
import os

#import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

import ccxt
import time
import pandas as pd

@dag(
    dag_id="process_crypto_data",
    #schedule_interval="*/2 * * * *",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def Process():
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
    def get_data():
        exchange = ccxt.binance()
        symbol = 'BTC/USD'
        timeframe = '1h'
        since = exchange.parse8601('2024-01-01T00:00:00Z')
        all_ohlcvs = []

        while True:
            try:
                ohlcvs = exchange.fetch_ohlcv(symbol, timeframe, since)
                all_ohlcvs += ohlcvs
                if len(ohlcvs):
                    print('Fetched', len(ohlcvs), symbol, timeframe, 'candles from', exchange.iso8601(ohlcvs[0][0]))
                    since = ohlcvs[-1][0] + 1
                    sleep_interval = exchange.rateLimit / 1000
                    print('Sleep for', sleep_interval)
                    time.sleep(sleep_interval)
                else:
                    break
            except Exception as e:
                print(type(e).__name__, str(e))
        # print('Fetched', len(all_ohlcvs), symbol, timeframe, 'candles in total')

        df = pd.DataFrame(all_ohlcvs)
        df.columns = ['date', 'open', 'high', 'low', 'close', 'volume']
        df = df.sort_values(by='date')
        df = df.drop_duplicates(subset='date').reset_index(drop=True)
        df['date'] = pd.to_datetime(df['date'], unit='ms')
        df.to_csv('BTC_USD_1h.csv')
        return df

    @task
    def transform_data():
        return

    @task
    def save_data():
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




dag = Process()