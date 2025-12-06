import pandas as pd

# Task: Create a single DataFrame where the index is the timestamp,
# and the columns represent the Best Bid and Best Ask for every venue (BME, XMAD, CBOE, etc.).

venue_pattern = '^(.*?)_\w+\d*$'
ask_columns = [f"px_ask_{i}" for i in range(0, 10)]
bid_columns = [f"px_bid_{i}" for i in range(0, 10)]

def consolidate_venues(dataframe: pd.DataFrame, venues) -> pd.DataFrame:
    _dataframe = dataframe.set_index('epoch', drop=False)
    venues_df = pd.DataFrame(index=_dataframe.index)

    for venue in venues:

        # El cambio de moneda varia mas rapido que el de los precios por lo que no deberia
        # tenerse en cuenta.
        currency_col = f'{venue}_currency'
        best_ask_col = f'{venue}_best_ask'
        best_bid_col = f'{venue}_best_bid'
        qty_for_best_ask_col = f'{venue}_qty_for_best_ask'
        qty_for_best_bid_col = f'{venue}_qty_for_best_bid'

        venue_df = _dataframe[_dataframe['mic'] == venue]

        #TODO uncomment venues_df[currency_col] = venue_df['currency']
        best_asks = venue_df[ask_columns].min(axis=1)
        best_bids = venue_df[bid_columns].max(axis=1)
        best_asks.name = best_ask_col
        best_bids.name = best_bid_col
        best_asks['epoch'] = venue_df['epoch']
        best_bids['epoch'] = venue_df['epoch']

        # Revisar outer
        venues_df = venues_df.join(best_asks, how='outer')
        venues_df = venues_df.join(best_bids, how='outer')

    return venues_df

def find_arbitrage(dataframe: pd.DataFrame, venues, latency=0, only_different_venues = False):
    best_ask_cols = [f'{venue}_best_ask' for venue in venues]
    best_bid_cols = [f'{venue}_best_bid'  for venue in venues]
    # Reduce the index (epoch) from microseconds to milliseconds
    arbitrages = dataframe.set_index(dataframe.index // 1000)

    # La idea es quitarlo del microsegundo, porque puede estar activo a n momento, en diferentes mercados, por uinos segundos
    Eso es lo que nos falta del not arbitraje
    Por ejemplo, XMAD 19:00:00:01 -> 90€ AQEU 19:00:01:00 -> 98 €
    No se van a ver por ese segundo de mierda
    Hayq ue ver como juntarlo

    Luego para las latencias habra que ver si mi tiempo mas la latencia que tengo es menor que el tiempo siguiente
    En ducho caso podre sacarlo, si no no

    # Get global Max and Min and compare them
    max_bid_per_epoch = dataframe[best_bid_cols].max(axis=1)
    min_ask_per_epoch = dataframe[best_ask_cols].min(axis=1)

    arbitrages = dataframe[max_bid_per_epoch > min_ask_per_epoch]
    if (only_different_venues):
        arbitrages
