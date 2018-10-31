from etk.timeseries.simple_annotator import SimpleAnnotator
from etk.timeseries.extract_spreadsheet import ExtractSpreadsheet


class SimpleTimeSeriesExtractor(object):
    """
    This class will take a "simple" csv or excel file as input and out a timeseries, if there is a timeseries to be
    extracted.
    """

    def __init__(self, csv_file_path: str):
        self.csv_file_path = csv_file_path

    def process(self):
        simple_annotator = SimpleAnnotator(self.csv_file_path)
        # returns the annotation object for the csv
        annotations = simple_annotator.get_annotation_json()
        es = ExtractSpreadsheet(self.csv_file_path, annotations=annotations)
        return list(es.process())


# if __name__ == '__main__':
#     import json
#     ste = SimpleTimeSeriesExtractor('/Users/amandeep/Github/sage-research-tool/datasets/fred_US_AL/example/DEXUSAL.csv')
#     print(json.dumps(ste.process(), indent=2))
