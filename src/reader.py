import os
import re
import pandas as pd
from pathlib import Path

# Codes for continues trading
ctc = {
    'AQUIS': [5308427],
    'BME': [5832713, 5832756],
    'CBOE': [12255233],
    'TURQUOISE': [7608181]
}

# Codes for invalid prices
price_rejected_values = [666666.666, 999999.999, 999999.989, 999999.988, 999999.979, 999999.123]

bid_columns = [f'px_bid_{i}' for i in range(0, 10)]
ask_columns = [f'px_ask_{i}' for i in range(0, 10)]
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
                dataframes['count'] = dataframes['count'] + 1 if 'count' in dataframes else 1
                df['venue'] = current.split('/')[-1].split('_')[0]
                if not type in dataframes:
                    dataframes[type] = df
                else:
                    dataframes[type] = pd.concat([dataframes[type], df])

def __load_dfs__(path, file_regex:str):
    dataframes = {}
    __load_dfs_rec__(path, file_regex, dataframes)
    return dataframes

def __find_continuos_trading_epochs__(dataframe):
    codes = ctc[dataframe['venue'].unique()[0]]
    venue_df = dataframe[dataframe['market_trading_status'].isin(codes)]
    opening_time = venue_df['epoch'].min()
    # Once we've found the opening time, we know next code its when market was closed
    opening_time_idx = dataframe[dataframe['epoch'] == opening_time].index
    # Get the int value
    closing_time = dataframe.loc[opening_time_idx+1]['epoch'].max()
    return [opening_time, closing_time]

def __clean_dfs__(dataframes):
    conditions = None
    mics = dataframes['QTE']['mic'].unique()
    # Remove invalid prices
    dataframes['QTE'] = dataframes['QTE'][~dataframes['QTE'][bid_columns].isin(price_rejected_values).any(axis=1)]
    dataframes['QTE'] = dataframes['QTE'][~dataframes['QTE'][ask_columns].isin(price_rejected_values).any(axis=1)]
    for mic in mics:
        sts = dataframes['STS'][dataframes['STS']['mic'] == mic]
        # Find opening and closing times for when the markets are opened
        market_range = __find_continuos_trading_epochs__(sts)

        # Remove the trades that were not done between the opening and closing times
        if conditions is None:
            conditions = ((dataframes['QTE']['mic'] == mic) & (dataframes['QTE']['epoch'] >= market_range[0]) & (dataframes['QTE']['epoch'] < market_range[1]))
        else:
            conditions = conditions | ((dataframes['QTE']['mic'] == mic) & (dataframes['QTE']['epoch'] >= market_range[0]) & (dataframes['QTE']['epoch'] < market_range[1]))

    dataframes['QTE'] = dataframes['QTE'][conditions]

    return dataframes

def load_for_isin(ISIN: str, use_small_data=False, debug=False):
    data_path = 'DATA_SMALL' if use_small_data else 'DATA_BIG'
    file_regex = file_pattern.replace('%ISIN%', ISIN)
    path = __get_dir__(data_path, Path(__file__))

    dataframes = __load_dfs__(path, file_regex)
    if 'QTE' in dataframes and 'STS' in dataframes:
        if debug:
            print(f"Read {dataframes['count']} files for ISIN: {ISIN}")

        dataframes = __clean_dfs__(dataframes)
        return dataframes['QTE']

    return None

def __get_isins_rec__(current, isins):
    for name in os.listdir(current):
        descriptor = os.path.join(current, name)
        if os.path.isdir(descriptor):
            __get_isins_rec__(descriptor, isins)
        else:
            file_info = re.match('(QTE|STS)_([0-9\-]+)_(.*?)_(.*?)_(.*?)_1\.csv\.gz', name)
            if file_info:
                isin = file_info.group(3)
                isins[isin] = True

def __get_isins__(path):
    isins = {}
    __get_isins_rec__(path, isins)
    return list(isins.keys())

def get_all_isins(use_small_data=False):
    data_path = 'DATA_SMALL' if use_small_data else 'DATA_BIG'
    path = __get_dir__(data_path, Path(__file__))
    return __get_isins__(path)

