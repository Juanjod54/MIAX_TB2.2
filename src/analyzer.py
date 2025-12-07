import pandas as pd

# Task: Create a single DataFrame where the index is the timestamp,
# and the columns represent the Best Bid and Best Ask for every venue (BME, XMAD, CBOE, etc.).

venue_pattern = '^(.*?)_\w+\d*$'
ask_columns = [f"px_ask_{i}" for i in range(0, 10)]
bid_columns = [f"px_bid_{i}" for i in range(0, 10)]

def __consolidate_venues__(dataframe: pd.DataFrame, venues, tolerance=1000) -> pd.DataFrame:
    tolerance = pd.Timedelta(microseconds=tolerance)
    _dataframe = dataframe.copy()
    _dataframe['date_time'] = pd.to_datetime(_dataframe['epoch'].astype('int64'), unit='us')
    venues_df = pd.DataFrame(_dataframe['date_time']).drop_duplicates(subset='date_time', keep='last')
    for venue in venues:

        # El cambio de moneda varia mas rapido que el de los precios por lo que no deberia
        # tenerse en cuenta.
        # currency_col = f'{venue}_currency'

        best_ask_col = f'{venue}_best_ask'
        best_bid_col = f'{venue}_best_bid'
        best_ask_vol_col = f'{venue}_best_ask_volume'
        best_bid_vol_col = f'{venue}_best_bid_volume'

        # En el libro de ordenes las filas vienen ordenadas
        # bid 0 -> mas alta, ask 0 -> mas baja

        venue_df = _dataframe[_dataframe['mic'] == venue]
        best_asks = venue_df['px_ask_0']
        best_bids = venue_df['px_bid_0']
        best_ask_volumes = venue_df['qty_ask_0']
        best_bid_volumes = venue_df['qty_bid_0']
        consolidated = pd.DataFrame(
            {
                best_ask_col: best_asks,
                best_bid_col: best_bids,
                best_ask_vol_col: best_ask_volumes,
                best_bid_vol_col: best_bid_volumes,
                'date_time': venue_df['date_time']
            }
        )

        venues_df = venues_df.sort_values('date_time')
        consolidated = consolidated.drop_duplicates(subset='date_time', keep='last').sort_values('date_time')
        venues_df = pd.merge_asof(venues_df, consolidated, on='date_time', direction='backward', tolerance=tolerance)

    venues_df = venues_df.set_index('date_time')
    return venues_df

def find_arbitrage(dataframe: pd.DataFrame, latency=1):
    venues = dataframe['mic'].unique()
    best_ask_cols = [f'{venue}_best_ask' for venue in venues]
    best_bid_cols = [f'{venue}_best_bid' for venue in venues]
    # Formamos el dataframe con el indice = epoch y la mejor orden de compra y venta por venue
    consolidated_dataframe = __consolidate_venues__(dataframe, venues)
    # Hacemos ffill para mantener los precios entre los diferentes timestamps
    # consolidated_dataframe = consolidated_dataframe.ffill()

    min_ask_per_epoch = consolidated_dataframe[best_ask_cols].min(axis=1)
    max_bid_per_epoch = consolidated_dataframe[best_bid_cols].max(axis=1)
    # Nos quedamos con los que pueden ser arbitrajes para reducir la busqueda
    possible_arbitrages = consolidated_dataframe.loc[(consolidated_dataframe[max_bid_per_epoch > min_ask_per_epoch]).index]
    # Obtenemos donde estan los min ask y max bid por epoch
    min_ask_venue_per_epoch = possible_arbitrages[best_ask_cols].idxmin(axis=1, skipna=True)
    max_bid_venue_per_epoch = possible_arbitrages[best_bid_cols].idxmax(axis=1, skipna=True)
    # Sacamos los venues
    min_ask_venue_per_epoch = min_ask_venue_per_epoch.apply(lambda v: v.split('_')[0] if pd.notna(v) else None)
    max_bid_venue_per_epoch = max_bid_venue_per_epoch.apply(lambda v: v.split('_')[0] if pd.notna(v) else None)
    # Nos quedamos solo con los que no sean del mismo venue
    possible_arbitrages = possible_arbitrages[max_bid_venue_per_epoch != min_ask_venue_per_epoch]

    arbitrages = pd.DataFrame(columns=['Ask', 'Bid', 'Volume', 'Profit'], index=possible_arbitrages.index)

    for epoch, row in possible_arbitrages.iterrows():
        # Sacamos el venue donde esta el menor precio de venta
        ask_venue = min_ask_venue_per_epoch[epoch]
        # Sacamos el venue donde esta el mayor precio de compra
        bid_venue = max_bid_venue_per_epoch[epoch]
        # Calculamos los valores
        min_ask = row[f'{ask_venue}_best_ask']
        max_bid = row[f'{bid_venue}_best_bid']
        ask_qty = row[f'{ask_venue}_best_ask_volume']
        bid_qty = row[f'{bid_venue}_best_bid_volume']

        arbitrages.loc[epoch] = [min_ask, max_bid, min(ask_qty, bid_qty), (max_bid - min_ask) * min(ask_qty, bid_qty)]

    # Todo revisar duplicados
    arbitrages = arbitrages.groupby(pd.Grouper(freq=f'{latency}us')).first()

    return arbitrages

