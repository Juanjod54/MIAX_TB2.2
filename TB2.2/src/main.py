import reader
import analyzer

dataframes = reader.load_for_isin('ES0113900J37', True)
venues = dataframes['QTE']['mic'].unique()
consolidated = analyzer.consolidate_venues(dataframes['QTE'], venues)
arbitrages = analyzer.find_arbitrage(consolidated, venues)

quantities = arbitrages.join(dataframes['QTE'], how='left')
