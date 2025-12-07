import reader
import analyzer

dataframes = reader.load_for_isin('ES0177542018', False)
arbitrages = analyzer.find_arbitrage(dataframes)