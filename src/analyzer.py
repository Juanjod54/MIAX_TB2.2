import pandas as pd

# Task: Create a single DataFrame where the index is the timestamp,
# and the columns represent the Best Bid and Best Ask for every venue (BME, XMAD, CBOE, etc.).

def consolidate_venues(dataframe: pd.DataFrame) -> pd.DataFrame:
    venues = dataframe['mic'].unique()
    operations_at_same_time = None

    # ISIN, mic, epoch, currency, ord_bid_x, px_bid_x, qty_bid_x, ord_ask_x, px_ask_x, qty_ask_0
    fixed_columns = ['isin', 'currency', 'epoch']
    level_columns = [f"{prefix}_{i}" for i in range(0, 10) for prefix in ['ord_bid', 'px_bid', 'qty_bid', 'ord_ask', 'px_ask', 'qty_ask']]
    columns = fixed_columns + level_columns

    for venue in venues:
        renamed_columns = [f"{venue}_{col}" for col in level_columns]
        venue_df = dataframe[dataframe['mic'] == venue][columns]
        venue_df = venue_df.rename(columns = {old: new for old, new in zip(level_columns, renamed_columns)})
        if operations_at_same_time is None:
            operations_at_same_time = venue_df
        else:
            operations_at_same_time = operations_at_same_time.merge(venue_df, on=['isin', 'epoch'])

    print(operations_at_same_time)
    return operations_at_same_time