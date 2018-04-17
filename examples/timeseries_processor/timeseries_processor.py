import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from etk.etk import ETK
from etk.etk_module import ETKModule
from etk.timeseries_processor import TimeseriesProcessor
import pprint


class TimeseriesETKModule(ETKModule):
    """
       Abstract class for extraction module
       """

    def __init__(self, etk):
        ETKModule.__init__(self, etk)

    def process_document(self, doc):
        pass

if __name__ == "__main__":
    etk = ETK(modules=TimeseriesETKModule)

    annotation = './resources/DIESEL_june_annotation.json'
    spreadsheet = './resources/DIESEL_june_2017.xlsx'
    timeseries_processor = TimeseriesProcessor(etk=etk, annotation=annotation, spreadsheet=spreadsheet)
    file_name = 'test_file_name'
    data_set = 'test_data_set'

    docs = [doc.cdr_document for doc in timeseries_processor.timeseries_extractor(file_name=file_name, data_set=data_set)]
    pprint.pprint(docs)