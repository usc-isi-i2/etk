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
        self.date_extractor = DateExtractor(self.etk, 'acled_date_parser')

    def process_document(self, doc: Document):
        if self.document_selector(doc):
            event_date = doc.select_segments(jsonpath='$.event_date')
            for segment in event_date:
                extractions = doc.extract(extractor=self.date_extractor, extractable=segment)
                # doc.store(extractions=extractions, attribute=self.date_extractor.name)
                # doc.kg.add_doc_value("event_date", "$.{}[*]".format(self.date_extractor.name))

                for extraction in extractions:
                    doc.kg.add_value("event_date", value=extraction.value)

            # for segment in doc.select_segments(jsonpath='$.notes'):
            #     doc.kg.add_value("description", segment.value)
            doc.kg.add_value("description", json_path='$.notes')

    def document_selector(self, doc) -> bool:
        """
        Boolean function for selecting document
        Args:
            doc: Document

        Returns:

        """
        return DefaultDocumentSelector().select_document(doc)

if __name__ == "__main__":

    kg_schema = KGSchema(json.load(open('master_config.json')))
    etk = ETK(modules=AcledModule, kg_schema=kg_schema)
    cp = CsvProcessor(etk=etk,
                      heading_row=1)

    data_set = 'test_data_set_csv'
    docs = cp.tabular_extractor(filename="acled_raw_data.csv", dataset='acled', doc_id_field="data_id")

    results = etk.process_ems(docs[0])

    print(json.dumps(results[0].value, indent=2))