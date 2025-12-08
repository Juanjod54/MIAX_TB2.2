import numpy as np
import pandas as pd

arbitrage_cols = ['Ask', 'Bid', 'From', 'To', 'Volume', 'Profit']

def __consolidate_mics__(dataframe: pd.DataFrame, mics) -> pd.DataFrame:
    tolerance = pd.Timedelta(microseconds=1000)
    _dataframe = dataframe.copy()[['mic', 'px_ask_0', 'px_bid_0', 'qty_ask_0', 'qty_bid_0']]
    _dataframe['date_time'] = pd.to_datetime(dataframe['epoch'].astype('int64'), unit='us')

    # Ordenamos por el momento en el que se registro el evento y quitamos duplicados para tener un df limpio
    _dataframe = _dataframe.sort_values('date_time')
    bests_df = pd.DataFrame(_dataframe['date_time']).drop_duplicates(subset='date_time', keep='last')
    for mic in mics:

        # El cambio de moneda varia mas rapido que el de los precios por lo que no deberia
        # tenerse en cuenta.
        # currency_col = f'{mic}_currency'

        best_ask_col = f'{mic}_best_ask'
        best_bid_col = f'{mic}_best_bid'
        best_ask_vol_col = f'{mic}_best_ask_volume'
        best_bid_vol_col = f'{mic}_best_bid_volume'

        # En el libro de ordenes las filas vienen ordenadas
        # bid 0 -> mas alta, ask 0 -> mas baja

        mic_df = _dataframe[_dataframe['mic'] == mic]

        best_asks = mic_df['px_ask_0']
        best_bids = mic_df['px_bid_0']
        best_ask_volumes = mic_df['qty_ask_0']
        best_bid_volumes = mic_df['qty_bid_0']
        consolidated = pd.DataFrame(
            {
                best_ask_col: best_asks,
                best_bid_col: best_bids,
                best_ask_vol_col: best_ask_volumes,
                best_bid_vol_col: best_bid_volumes,
                'date_time': mic_df['date_time']
            }
        )

        bests_df = bests_df.sort_values('date_time')
        bests_df = pd.merge_asof(bests_df, consolidated, on='date_time', direction='backward', tolerance=tolerance)

    mics_df = bests_df.set_index('date_time')
    return mics_df

def find_arbitrage(dataframe: pd.DataFrame, latency=0):
    mics = dataframe['mic'].unique()
    best_ask_cols = [f'{mic}_best_ask' for mic in mics]
    best_bid_cols = [f'{mic}_best_bid' for mic in mics]
    best_bid_qty_cols = [f'{mic}_best_bid_volume' for mic in mics]
    # Formamos el dataframe con el indice = epoch y la mejor orden de compra y venta por mic, con una tolerancia de hasta 1 ms entre mics
    consolidated_dataframe = __consolidate_mics__(dataframe, mics)

    if latency:
        # Aplicamos la latencia
        affected_columns = best_bid_cols + best_bid_qty_cols
        # Sustituimos los valores de compra (a los que les afecta la latencia) por sus valores en 'latency' microsegundos
        shifted_bids = consolidated_dataframe[affected_columns].shift(freq=pd.Timedelta(microseconds=latency))
        shifted_bids = shifted_bids.reindex(consolidated_dataframe.index, fill_value=np.nan)
        consolidated_dataframe[affected_columns] = shifted_bids

    min_ask_per_epoch = consolidated_dataframe[best_ask_cols].min(axis=1)
    max_bid_per_epoch = consolidated_dataframe[best_bid_cols].max(axis=1)

    # Nos quedamos con los que pueden ser arbitrajes para reducir la busqueda
    bid_gt_ask = (max_bid_per_epoch > min_ask_per_epoch)
    bid_gt_ask = bid_gt_ask.reindex(consolidated_dataframe.index, fill_value=False)
    possible_arbitrages = consolidated_dataframe[bid_gt_ask]

    # Obtenemos donde estan los min ask y max bid por epoch
    min_ask_mic_per_epoch = possible_arbitrages[best_ask_cols].idxmin(axis=1, skipna=True)
    max_bid_mic_per_epoch = possible_arbitrages[best_bid_cols].idxmax(axis=1, skipna=True)

    # Sacamos los mics
    min_ask_mic_per_epoch = min_ask_mic_per_epoch.apply(lambda v: v.split('_')[0] if pd.notna(v) else None)
    max_bid_mic_per_epoch = max_bid_mic_per_epoch.apply(lambda v: v.split('_')[0] if pd.notna(v) else None)

    # Nos quedamos solo con los que no sean del mismo mic
    different_mic = (max_bid_mic_per_epoch != min_ask_mic_per_epoch)
    different_mic = different_mic.reindex(possible_arbitrages.index, fill_value=False)
    possible_arbitrages = possible_arbitrages[different_mic]

    last_arbitrage = None
    first_occur_at = None
    max_delay = pd.Timedelta(seconds=1)
    arbitrages = pd.DataFrame(columns=arbitrage_cols, index=possible_arbitrages.index)
    for epoch, row in possible_arbitrages.iterrows():

        # Sacamos el mic donde esta el menor precio de venta
        ask_mic = min_ask_mic_per_epoch[epoch]
        # Sacamos el mic donde esta el mayor precio de compra
        bid_mic = max_bid_mic_per_epoch[epoch]
        # Calculamos los valores
        min_ask = row[f'{ask_mic}_best_ask']
        max_bid = row[f'{bid_mic}_best_bid']
        ask_qty = row[f'{ask_mic}_best_ask_volume']
        bid_qty = row[f'{bid_mic}_best_bid_volume']

        arbitrage = [min_ask, max_bid, ask_mic, bid_mic, min(ask_qty, bid_qty),
                                 (max_bid - min_ask) * min(ask_qty, bid_qty)]

        ################################################################################
        ## In a simulation, if an opportunity persists for 1 second (1000 snapshots), ##
        ## you can only trade it once (the first time it appears)                     ##
        ## if the opportunity vanishes and quickly reappears you can count it         ##
        ## as a new opportunity for simplification                                    ##
        ################################################################################

        if (last_arbitrage is None) or (last_arbitrage != arbitrage) or ((first_occur_at + max_delay) < epoch):
            first_occur_at = epoch
            last_arbitrage = arbitrage

            arbitrages.loc[epoch] = arbitrage

    ################################################################################

    ################################################################################
    ##                              Alternativa                                   ##
    ################################################################################
    ## In a simulation, if an opportunity persists for 1 second (1000 snapshots), ##
    ## you can only trade it once (the first time it appears)                     ##
    ## if the opportunity vanishes and quickly reappears you can count it         ##
    ## as a new opportunity for simplification                                    ##
    ################################################################################

    # Eliminamos filas repetidas en menos de 1 segundo
    # time_delta = possible_arbitrages.index.to_series().diff()
    # duplicated = (arbitrages[['Ask', 'Bid', 'From', 'To', 'Volume', 'Profit']] == arbitrages[['Ask', 'Bid', 'From', 'To', 'Volume', 'Profit']].shift(1)).all(axis=1)
    # arbitrages = arbitrages[~ (duplicated & time_delta < max_delay)]
    ################################################################################

    return arbitrages



