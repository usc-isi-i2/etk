from etk.simple_timeseries_extractor import SimpleTimeSeriesExtractor
if __name__ == '__main__':
    import json
    ste = SimpleTimeSeriesExtractor('/Users/amandeep/Github/sage-research-tool/datasets/fred_US_AL/example/DEXUSAL.csv')
    print(json.dumps(ste.process(), indent=2))