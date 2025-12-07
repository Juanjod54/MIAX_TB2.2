import pandas as pd

# Task: Create a single DataFrame where the index is the timestamp,
# and the columns represent the Best Bid and Best Ask for every venue (BME, XMAD, CBOE, etc.).

venue_pattern = '^(.*?)_\w+\d*$'
ask_columns = [f"px_ask_{i}" for i in range(0, 10)]
bid_columns = [f"px_bid_{i}" for i in range(0, 10)]

def __consolidate_venues__(dataframe: pd.DataFrame, venues) -> pd.DataFrame:
    _dataframe = dataframe.set_index('epoch')
    venues_df = pd.DataFrame(index=_dataframe.index)

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
        consolidated = pd.DataFrame({best_ask_col: best_asks, best_bid_col: best_bids, best_ask_vol_col: best_ask_volumes, best_bid_vol_col: best_bid_volumes})

        venues_df = venues_df.join(consolidated, how='outer')

    return venues_df

def find_arbitrage(dataframe: pd.DataFrame, latency=0):
    venues = dataframe['mic'].unique()
    best_ask_cols = [f'{venue}_best_ask' for venue in venues]
    best_bid_cols = [f'{venue}_best_bid' for venue in venues]
    # Formamos el dataframe con el indice = epoch y la mejor orden de compra y venta por venue
    consolidated_dataframe = __consolidate_venues__(dataframe, venues)
    # Hacemos ffill para mantener los precios entre los diferentes timestamps
    consolidated_dataframe = consolidated_dataframe.ffill()

    min_ask_per_epoch = consolidated_dataframe[best_ask_cols].min(axis=1)
    max_bid_per_epoch = consolidated_dataframe[best_bid_cols].max(axis=1)
    # Nos quedamos con los que pueden ser arbitrajes para reducir la busqueda
    arbitrages = consolidated_dataframe.loc[(consolidated_dataframe[max_bid_per_epoch > min_ask_per_epoch]).index]
    # Obtenemos donde estan los min ask y max bid por epoch
    min_ask_venue_per_epoch = arbitrages[best_ask_cols].idxmin(axis=1, skipna=True)
    max_bid_venue_per_epoch = arbitrages[best_bid_cols].idxmax(axis=1, skipna=True)
    # Sacamos los venues
    min_ask_venue_per_epoch = min_ask_venue_per_epoch.apply(lambda v: v.split('_')[0] if pd.notna(v) else None)
    max_bid_venue_per_epoch = max_bid_venue_per_epoch.apply(lambda v: v.split('_')[0] if pd.notna(v) else None)
    # Nos quedamos solo con los que no sean del mismo venue
    arbitrages = arbitrages[max_bid_venue_per_epoch != min_ask_venue_per_epoch]

    profits = pd.DataFrame({'From': '-', 'To': '-', 'Profit': 0.0}, index=arbitrages.index)
    for epoch, row in arbitrages.iterrows():
        # Sacamos el venue donde esta el menor precio de venta
        ask_venue = min_ask_venue_per_epoch[epoch]
        # Sacamos el venue donde esta el mayor precio de compra
        bid_venue = max_bid_venue_per_epoch[epoch]
        # Calculamos los valores
        min_ask = row[f'{ask_venue}_best_ask']
        max_bid = row[f'{bid_venue}_best_bid']
        ask_qty = row[f'{bid_venue}_best_ask_volume']
        bid_qty = row[f'{bid_venue}_best_bid_volume']
        profits.loc[epoch, ['From', 'To', 'Profit']] = [ask_venue, bid_venue, (max_bid - min_ask) * min(ask_qty, bid_qty)]

    return profits


    # La idea es quitarlo del microsegundo, porque puede estar activo a n momento, en diferentes mercados, por unos segundos
    # Eso es lo que nos falta del not arbitraje
    # Por ejemplo, XMAD 19:00:00:01 -> 90€ AQEU 19:00:01:00 -> 98 €
    # No se van a ver por ese segundo de mierda
    # Hayq ue ver como juntarlo
    #
    # Luego para las latencias habra que ver si mi tiempo mas la latencia que tengo es menor que el tiempo siguiente
    # En ducho caso podre sacarlo, si no no

    # Hay que rellenar hasta que cambie de valor



    arbitrages = dataframe[max_bid_per_epoch > min_ask_per_epoch]
