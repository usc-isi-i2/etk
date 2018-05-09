import os
import sys, json
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from etk.etk import ETK
from etk.etk_module import ETKModule
from etk.csv_processor import CsvProcessor
from etk.extractors.date_extractor import DateExtractor
from etk.document_selector import DefaultDocumentSelector
from etk.document import Document
from etk.knowledge_graph_schema import KGSchema


class AcledModule(ETKModule):
    def __init__(self, etk):
        ETKModule.__init__(self, etk)
        # self.date_extractor = DateExtractor(self.etk, 'acled_date_parser')

    def process_document(self, doc: Document):
        event_date = doc.select_segments(jsonpath='$.event_date')
        for segment in event_date:
            extractions = doc.extract(extractor=self.date_extractor, extractable=segment)

            for extraction in extractions:
                doc.kg.add_value("event_date", extraction.value)

        doc.kg.add_doc_value("description", '$.notes')

    def document_selector(self, doc) -> bool:
        return DefaultDocumentSelector().select_document(doc, datasets=["ucdp"])


if __name__ == "__main__":

    kg_schema = KGSchema(json.load(open('master_config.json')))
    etk = ETK(modules=AcledModule, kg_schema=kg_schema)
    cp = CsvProcessor(etk=etk, heading_row=1)

    for doc in cp.tabular_extractor(filename="ucdp_sample.xls", data_set='ucdp'):
        doc.kg.add_doc_value("country", "$.Location")
        print(doc.kg.value, doc.cdr_document)

    # print(json.dumps(docs[0].cdr_document, indent=2))
    # results = etk.process_ems(docs[0])
    #
    # print(json.dumps(results[0].value, indent=2))