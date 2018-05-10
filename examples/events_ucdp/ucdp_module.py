import os
import sys, json
import datetime
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from etk.etk import ETK
from etk.etk_module import ETKModule
from etk.csv_processor import CsvProcessor
from etk.extractors.date_extractor import DateExtractor, DateResolution
from etk.extractors.decoding_value_extractor import DecodingValueExtractor
from etk.document_selector import DefaultDocumentSelector
from etk.document import Document
from etk.knowledge_graph_schema import KGSchema


class UCDPModule(ETKModule):

    event_prefix = "http://ontology.causeex.com/ontology/odps/Event#"

    incomp_type = {
        "1": "Territory Conflict",
        "2": "Government Conflict",
        "3": "Government and Territory Conflict",
    }

    int_event_type = {
        "0": "Minor Conflict",
        "1": "War"
    }

    int_causeex_type = {
        "1": event_prefix + "War"
    }

    def __init__(self, etk):
        ETKModule.__init__(self, etk)
        self.doc_selector = DefaultDocumentSelector()
        self.incomp_decoder = DecodingValueExtractor(self.incomp_type, 'Incomp Decoder')
        self.int_decoder = DecodingValueExtractor(self.int_event_type, 'Int Decoder')
        self.int_causeex_decoder = DecodingValueExtractor(self.int_causeex_type, 'Int CauseEx Type', default_action="delete")

    def fix_cdr_document(self, doc):
        """make all attributes strings"""
        cdr = doc.cdr_document
        for k, v in cdr.items():
            if isinstance(v, int):
                cdr[k] = str(v)
            elif isinstance(v, datetime.date):
                cdr[k] = DateExtractor.convert_to_iso_format(v, DateResolution.DAY)

    def process_document(self, doc: Document):
        self.fix_cdr_document(doc)

        doc.kg.add_doc_value("country", "$.Location")

        doc.store(doc.extract(self.incomp_decoder, doc.select_segments("$.Incomp")[0]), "incomp_type")
        doc.kg.add_doc_value("type", "$.incomp_type")

        doc.store(doc.extract(self.int_decoder, doc.select_segments("$.Int")[0]), "int_type")
        doc.kg.add_doc_value("type", "$.int_type")

        doc.kg.add_value("type", "Event")

        doc.store(doc.extract(self.int_causeex_decoder, doc.select_segments("$.Int")[0]), "int_causeex_class")
        doc.kg.add_doc_value("causeex_class", "$.int_causeex_class")

        doc.kg.add_value("causeex_class", self.event_prefix+"ArmedConflict")

        doc.kg.add_doc_value("event_date", "$.StartDate")

        print(json.dumps(doc.cdr_document, indent=2))
        print(json.dumps(doc.kg.value, indent=2))


        # event_date = doc.select_segments(jsonpath='$.event_date')
        # for segment in event_date:
        #     extractions = doc.extract(extractor=self.date_extractor, extractable=segment)
        #
        #     for extraction in extractions:
        #         doc.kg.add_value("event_date", extraction.value)
        #
        # doc.kg.add_doc_value("description", '$.notes')

        return []

    def document_selector(self, doc) -> bool:
        return self.doc_selector.select_document(doc, datasets=["ucdp"])


if __name__ == "__main__":

    kg_schema = KGSchema(json.load(open('master_config.json')))
    etk = ETK(modules=UCDPModule, kg_schema=kg_schema)
    cp = CsvProcessor(etk=etk, heading_row=1)

    for doc in cp.tabular_extractor(filename="ucdp_sample.xls", data_set='ucdp'):
        etk.process_ems(doc)
        # print(doc.cdr_document)
