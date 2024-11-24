import pandas as pd
import streamlit as st
import plotly.graph_objects as go
import os

#TODO сделать передаваемым параметром
ticker = 'BTC/USDT'
ticker_underscore = ticker.replace("/", "_")

def linear_plot(df, title):
    fig = go.Figure([go.Scatter(x=df['date'], y=df['close'], mode='lines')])
    fig.update_layout(plot_bgcolor='white',
                      xaxis_title='Date',
                      yaxis_title='Price',
                      title=title)
    return fig


def candlestick_plot(df, title):
    fig = go.Figure([go.Candlestick(x=df['date'],
                                    open=df['open'],
                                    high=df['high'],
                                    low=df['low'],
                                    close=df['close'])])
    fig.update_layout(xaxis_rangeslider_visible=False,
                      plot_bgcolor='white',
                      xaxis_title='Date',
                      yaxis_title='Price',
                      title=title)
    fig.update_yaxes(fixedrange=False)
    return fig

def load_data():
    postgresql_db_uri = os.environ.get('AIRFLOW_CONN_POSTGRES_DATA_STORAGE')
    if postgresql_db_uri is not None:
        try:
            return pd.read_sql_table(ticker_underscore, postgresql_db_uri)
        except Exception as e:
            print(type(e).__name__, str(e))

st.title("Market data dashboard")

with st.spinner('Download data...'):
    df = load_data()

if df is None:
    st.error("Data is not available")
else:
    st.header("Plots")
    st.plotly_chart(linear_plot(df, ticker))
    st.plotly_chart(candlestick_plot(df, ticker))

    st.header("Data")
    st.dataframe(df)