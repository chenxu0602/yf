from pandas.io import json
import yfinance as yf
import json
from pathlib import Path
import numpy as np
import pandas as pd


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
        res[inst] = data[inst]

    return res


def load_yf_5m(
    tickers: list[str],
) -> dict[str, pd.DataFrame]:
    return load_yf(tickers, interval='5m', period='60d')


def load_yf_1h(
    tickers: list[str],
) -> dict[str, pd.DataFrame]:
    return load_yf(tickers, interval='1h', period='2y')






