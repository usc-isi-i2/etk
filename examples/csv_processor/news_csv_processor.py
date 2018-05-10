import pprint
import os
import sys
import codecs
import json
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from etk.etk import ETK
from etk.etk_module import ETKModule
from etk.csv_processor import CsvProcessor


class CsvETKModule(ETKModule):
    """
       Abstract class for extraction module
       """

    def __init__(self, etk):
        ETKModule.__init__(self, etk)

    def process_document(self, doc):
        pass

if __name__ == "__main__":
    csv_str = """text,with,Polish,non-Latin,lettes
    1,2,3,4,5,6
    a,b,c,d,e,f
    """
    file_path = 'etk/unit_tests/ground_truth/queryResults.csv'
    jl_file_path = 'etk/unit_tests/ground_truth/queryResults.jl'
    etk = ETK(modules=CsvETKModule)
    csv_processor = CsvProcessor(etk=etk,
                                 heading_row=2,
                                 content_start_row=10,
                                 content_end_row=1723)

    data_set = 'test_data_set_csv'
    test_docs = [doc.cdr_document for doc in
                 csv_processor.tabular_extractor(filename=file_path, dataset='test_set')]
    """docs = [doc.cdr_document for doc in
                 cp.tabular_extractor(table_str=filename, data_set='test_csv_str_with_all_args')]"""
    news_path = 'etk/unit_tests/ground_truth/queryResults.jl'
    news_data = open(news_path, 'w')

    news_data.write(json.dumps(test_docs))
    #pprint.pprint(test_docs)