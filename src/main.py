import reader
import analyzer

dataframes = reader.load_for_isin('ES0113900J37', True)
arbitrages = analyzer.consolidate_venues(dataframes['QTE'])