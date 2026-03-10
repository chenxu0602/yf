import clickhouse_connect
from clickhouse_connect.driver.client import Client as CH_Client
import numpy as np
import pandas as pd


def get_clickhouse_client(
    host: str = 'localhost',
    # host: str = 'mini.local',
    username: str = 'admin',
    port: int = 8123) -> CH_Client:

    client = clickhouse_connect.get_client(host=host, username=username, port=port)
    return client


def show_clickhouse_databases(client: CH_Client = get_clickhouse_client()) -> str:
    return client.command('show databases').split('\n')


def create_database(db: str, client: CH_Client = get_clickhouse_client()) -> bool:

    drop_db_query = f'DROP DATABASE IF EXISTS {db}'
    client.command(drop_db_query)
    print(f'Database {db} dropped if it existed.')

    create_db_query = f'CREATE DATABASE {db}'
    client.command(create_db_query)
    print(f'Database {db} created.')

    return True


def create_yfinance_table(
    table: str, 
    db: str = 'yfinance',
    client: CH_Client = get_clickhouse_client()) -> bool:

    client.command(f'USE {db}')

    drop_table_query = f'DROP TABLE IF EXISTS {table}'
    client.command(drop_table_query)
    print(f'Table {table} dropped if it existed.')

    create_table_query = f"""
        CREATE TABLE {table} (
            Datetime                 DateTime('UTC'), 
            Symbol                   String,
            Open                     Float32,
            High                     Float32,
            Low                      Float32,
            Close                    Float32,
            Volume                   Float32,
            updated_at               DateTime64(3, 'UTC') DEFAULT now64(3),
        ) ENGINE = ReplacingMergeTree(updated_at)
        ORDER BY (Symbol, Datetime)
    """

    client.command(create_table_query)
    print(f'Table {table} created.')

    return True

