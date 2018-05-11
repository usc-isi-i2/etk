import pprint
import os
import sys
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

    gęś,zółty,wąż,idzie,wąską,dróżką,
    ,b,c,s,w,f
    """
    etk = ETK(modules=CsvETKModule)
    cp = CsvProcessor(etk=etk,
                        heading_row=1,
                        heading_columns=(1, 3),
                        content_end_row=3,
                        ends_with_blank_row=True,
                        remove_leading_empty_rows=True,
                        required_columns=['text'])

    data_set = 'test_data_set_csv'
    docs = [doc.cdr_document for doc in
            cp.tabular_extractor(table_str=csv_str, dataset='test_csv_str_with_all_args')]
    pprint.pprint(docs)