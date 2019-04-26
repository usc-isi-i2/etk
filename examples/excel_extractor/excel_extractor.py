import os, sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from etk.etk import ETK
from etk.etk_module import ETKModule
from etk.extractors.excel_extractor import ExcelExtractor
from etk.utilities import Utility


class ExampleETKModule(ETKModule):
    """
    Abstract class for extraction module
    """
    def __init__(self, etk):
        ETKModule.__init__(self, etk)
        self.ee = ExcelExtractor()

    def document_selector(self, doc):
        return 'file_path' in doc.cdr_document

    def process_document(self, doc):
        """
        Add your code for processing the document
        """

        variables = {
            'value': '$col,$row',  # cell itself
            'country': '$col-1,$row',  # relative offset
            'category': '$col,$row-1',  # relative offset
            'table': '$A,$1',  # fixed cell
            'from_row': '$row',  # single variable
            'from_col': '$col'  # single variable
        }

        raw_extractions = self.ee.extract(doc.cdr_document['file_path'], '16tbl08al', ['C,7', 'M,33'], variables)

        extracted_docs = []
        for d in raw_extractions:
            d['doc_id'] = Utility.create_doc_id_from_json(d)
            extracted_docs.append(etk.create_document(d))

        return extracted_docs


if __name__ == "__main__":
    etk = ETK(modules=ExampleETKModule)
    doc = etk.create_document({'file_path': 'alabama.xls'})
    docs = etk.process_ems(doc)

    for d in docs:
        print(d.value)

