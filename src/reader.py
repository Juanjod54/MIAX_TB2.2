import os
import re
from pathlib import Path

import pandas as pd

file_pattern = '(QTE|STS)_([0-9\-]+)_%ISIN%_(.*?)_(.*?)_1\.csv\.gz'

def __get_dir__(dirname, current):
    if current.name == '/':
        return None
    is_this_dir = current.name == dirname
    is_inside_current_dir = os.path.isdir(current) and dirname in os.listdir(current)
    if is_this_dir or is_inside_current_dir :
        return os.path.join(current, dirname)
    return __get_dir__(dirname, current.parent)

def __load_dfs_rec__(current, file_regex:str, dataframes: dict):
    for name in os.listdir(current):
        descriptor = os.path.join(current, name)
        if os.path.isdir(descriptor):
            __load_dfs_rec__(descriptor, file_regex, dataframes)
        else:
            file_info = re.match(file_regex, name)
            if file_info:
                type = file_info.group(1)
                df = pd.read_csv(descriptor, sep=';')
                if not type in dataframes:
                    dataframes[type] = df
                else:
                    dataframes[type] = pd.concat([dataframes[type], df])

def __load_dfs__(path, file_regex:str):
    dataframes = {}
    __load_dfs_rec__(path, file_regex, dataframes)
    return dataframes

def __clean_dfs__(dataframes):
    market_valid_values = [5308427, 5832713, 5832756, 12255233, 7608181]
    price_rejected_values = [666666.666, 999999.999, 999999.989, 999999.988, 999999.979, 999999.123]
    bid_columns = [f'px_bid_{i}' for i in range(0, 10)]
    ask_columns = [f'px_ask_{i}' for i in range(0, 10)]
    dataframes['STS'] = dataframes['STS'][dataframes['STS']['market_trading_status'].isin(market_valid_values)]
    dataframes['QTE'] = dataframes['QTE'][~dataframes['QTE'][bid_columns].isin(price_rejected_values).any(axis=1)]
    dataframes['QTE'] = dataframes['QTE'][~dataframes['QTE'][ask_columns].isin(price_rejected_values).any(axis=1)]

def load_for_isin(ISIN: str, use_small_data=False):
    data_path = 'DATA_SMALL' if use_small_data else 'DATA_BIG'
    file_regex = file_pattern.replace('%ISIN%', ISIN)
    path = __get_dir__(data_path, Path(__file__))

    dataframes = __load_dfs__(path, file_regex)
    __clean_dfs__(dataframes)
    return dataframes
