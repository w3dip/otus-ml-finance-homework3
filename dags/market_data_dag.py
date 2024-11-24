import pendulum
import ccxt
import time
import pandas as pd

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import timedelta

from feature_generator import *
from target_utils import *

@dag(
    dag_id="market_data_dag",
    schedule_interval="@daily",
    start_date=pendulum.now(tz="UTC"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
)
def process():

    #TODO сделать передаваемым параметром
    ticker = 'BTC/USDT'
    ticker_underscore = ticker.replace("/", "_")
    timeframe = '1h'
    filename = f'{ticker_underscore}_{timeframe}'

    def get_dest_file(file):
        return f'~/{file}.csv'

    @task
    def get_data(**kwargs):
        exchange = ccxt.binance()

        print('----------JOB intervals:----------')
        prev_data_interval_end_success: pendulum.DateTime = kwargs.get('prev_data_interval_end_success')
        if prev_data_interval_end_success is None:
            print('prev_data_interval_end_success (last successful run) is None')
            start_date_dt = pendulum.datetime(2024, 1, 1, tz="UTC")
        else:
            print('prev_data_interval_end_success (last successful run)', prev_data_interval_end_success.to_iso8601_string())
            start_date_dt = prev_data_interval_end_success

        start_date = exchange.parse8601(start_date_dt.to_iso8601_string())
        print("Current job start_date", start_date_dt.to_iso8601_string())

        end_date_dt: pendulum.DateTime = kwargs.get('data_interval_end')
        print("Current job end_date", end_date_dt.to_iso8601_string())
        end_date = exchange.parse8601(end_date_dt.to_iso8601_string())

        all_ohlcvs = []

        print('----------DOWNLOAD DATA:----------')
        while start_date < end_date:
            try:
                ohlcvs = exchange.fetch_ohlcv(ticker, timeframe, start_date)
                if len(ohlcvs):
                    print('Fetched', len(ohlcvs), ticker, timeframe, 'candles from', exchange.iso8601(ohlcvs[0][0]))
                    start_date = ohlcvs[-1][0] + 1

                    if ohlcvs[-1][0] >= end_date:
                        ohlcvs = filter(lambda x: x[0] < end_date, ohlcvs)

                    all_ohlcvs += ohlcvs
                    sleep_interval = exchange.rateLimit / 1000
                    print('Sleep for', sleep_interval)
                    time.sleep(sleep_interval)
                else:
                    break
            except Exception as e:
                print(type(e).__name__, str(e))
                break

        df = pd.DataFrame(all_ohlcvs, columns=['date', 'open', 'high', 'low', 'close', 'volume'])
        dest_file = get_dest_file(filename)
        df.to_csv(dest_file, index=False)
        return dest_file

    @task
    def clean(source_file):
        df = pd.read_csv(source_file)
        df = df.sort_values(by='date')
        df = df.drop_duplicates(subset='date').reset_index(drop=True)
        df['date'] = pd.to_datetime(df['date'], unit='ms')
        df = df.set_index('date')

        # Заполним пропуски если они есть
        df = df.interpolate(method='polynomial', order=2)

        dest_file = get_dest_file(filename + '_cleaned')
        df.to_csv(dest_file)
        return dest_file

    @task
    def add_features(source_file):
        df = pd.read_csv(source_file)
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date')

        features = ['open', 'high', 'low', 'close', 'volume']

        # Добавление лагов
        lag_periods = 3
        df_with_lags, new_columns = create_lag_features(df, features, lag_periods)

        # Добавляем статистики по окну
        window_sizes = [5, 14, 30]
        df_with_rolling, new_rolling_features = create_rolling_features(df_with_lags, features, window_sizes)

        # Добавляем трендовые фичи и тех. индикаторы
        df_with_trend, new_trend_features = create_trend_features(df_with_rolling, features, lag_periods)

        # Добавляем MACD для признака 'close'
        df_with_trend, macd_column = create_macd(df_with_trend, 'close')

        # Добавляем целевую переменную (таргет)
        df = filter_invalid_targets(add_target(df_with_trend))

        dest_file = get_dest_file(filename + '_with_new_features')
        df.to_csv(dest_file)
        return dest_file

    @task
    def save_to_database(source_file):
        df = pd.read_csv(source_file)
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date')
        postgres_hook = PostgresHook(postgres_conn_id="postgres_data_storage")
        df.to_sql(ticker_underscore, postgres_hook.get_sqlalchemy_engine(), if_exists='append', chunksize=1000)
        return

    original_data = get_data()
    cleaned_data = clean(original_data)
    data_with_new_features = add_features(cleaned_data)
    save_to_database(data_with_new_features)

dag = process()