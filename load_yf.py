from pandas.io import json
import yfinance as yf
import json
from pathlib import Path
import numpy as np
import pandas as pd
import clickhouse_connect
from clickhouse_connect.driver.client import Client as CH_Client


def load_symbols(
    symbols_file_path: str = "symbols.json",
) -> list[str]:

    symbols_file = Path(__file__).resolve().parent / symbols_file_path
    with open(symbols_file, 'r') as f:
        data = json.load(f)

    res = []
    for k, v in data.items():
        res += f'{k}{v}',

    return sorted(res)

FIELDS = ['Datetime', 'Symbol', 'Open', 'High', 'Low', 'Close', 'Volume']

def load_yf(
    tickers: list[str],
    period: str = '60d',
    interval: str = '5m',
    group_by: str = 'ticker',
    threads: bool = True,
) -> dict[str, pd.DataFrame]:
    data = yf.download(
        tickers=tickers,
        period=period,
        interval=interval,
        group_by=group_by,
        threads=threads  # Enables multi-threaded downloading for speed
    )

    insts = set(data.columns.get_level_values(0))
    res: dict[str, pd.DataFrame] = {}

    for inst in insts:
        res[inst] = data[inst].dropna(how='all')

    return res


def load_yf_5m(
    tickers: list[str],
) -> dict[str, pd.DataFrame]:
    return load_yf(tickers, interval='5m', period='60d')


def load_yf_1h(
    tickers: list[str],
) -> dict[str, pd.DataFrame]:
    return load_yf(tickers, interval='1h', period='2y')


def load_yr_1h_and_save(
    tickers: list[str] = load_symbols(),
    client: CH_Client = clickhouse_connect.get_client(host='localhost', username='admin', port=8123),
) -> dict[str, pd.DataFrame]:

    data = load_yf_1h(tickers)
    for ticker in tickers:
        try:
            df = data[ticker]
        except Exception as e:
            print(f"Error loading data for {ticker}: {e}")
            continue
        else:
            if df.empty:
                print(f"No data for {ticker}")
                continue
            sym= ticker.split('=')[0]
            if not '=' in ticker:
                sym = ticker.split('-')[0]
            df['Symbol'] = sym

        print(f'Saving data for {ticker} to ClickHouse ...')
        print(df.tail(10))
        try:
            client.insert_df('yfinance.yfinance_1h', df.reset_index()[FIELDS])
        except Exception as e:
            print(f"Error saving data for {ticker}: {e}")
        else:
            print(f"Data for {ticker} saved successfully.")

    return data


def load_yr_5m_and_save(
    tickers: list[str] = load_symbols(),
    client: CH_Client = clickhouse_connect.get_client(host='localhost', username='admin', port=8123),
) -> dict[str, pd.DataFrame]:

    data = load_yf_5m(tickers)
    for ticker in tickers:
        try:
            df = data[ticker]
        except Exception as e:
            print(f"Error loading data for {ticker}: {e}")
            continue
        else:
            if df.empty:
                print(f"No data for {ticker}")
                continue
            sym= ticker.split('=')[0]
            if not '=' in ticker:
                sym = ticker.split('-')[0]
            df['Symbol'] = sym

        print(f'Saving data for {ticker} to ClickHouse...')
        try:
            client.insert_df('yfinance.yfinance_5m', df.reset_index()[FIELDS])
        except Exception as e:
            print(f"Error saving data for {ticker}: {e}")
        else:
            print(f"Data for {ticker} saved successfully.")

    return data

















