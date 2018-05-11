import os
import sys, json
from typing import List
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from etk.etk import ETK
from etk.etk_module import ETKModule
from etk.csv_processor import CsvProcessor
from etk.extractors.decoding_value_extractor import DecodingValueExtractor
from etk.document_selector import DefaultDocumentSelector
from etk.document import Document
from etk.knowledge_graph_schema import KGSchema
from etk.utilities import Utility


class UCDPActorModule(ETKModule):

    def __init__(self, etk):
        ETKModule.__init__(self, etk)

    def document_selector(self, doc) -> bool:
        return doc.cdr_document["dataset"] == "ucdp-actor"

    def process_document(self, doc: Document) -> List[Document]:
        doc.kg.add_doc_value("country", "$.SideA")
        doc.kg.add_value("type", "Group")
        doc.kg.add_value("type", "Country")
        return []


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

    def process_document(self, doc: Document) -> List[Document]:
        Utility.make_json_serializable(doc.cdr_document)
        doc.doc_id = Utility.create_doc_id_from_json(doc.cdr_document)

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

        actor1_dict = {
            "SideA": doc.cdr_document["SideA"],
            "dataset": "ucdp-actor"
        }
        actor1_doc = etk.create_document(actor1_dict)
        actor1_doc.doc_id = doc.doc_id + "_actor1"
        doc.kg.add_value("actor", actor1_doc.doc_id)

        actor2_dict = {
            "SideB": doc.cdr_document["SideB"],
            "dataset": "ucdp-actor"
        }
        actor2_doc = etk.create_document(actor2_dict)
        actor2_doc.doc_id = doc.doc_id + "_actor2"
        doc.kg.add_value("actor", actor2_doc.doc_id)
        return [
            actor1_doc,
            actor2_doc
        ]

    def document_selector(self, doc) -> bool:
        # return self.doc_selector.select_document(doc, datasets=["ucdp"])
        return doc.cdr_document["dataset"] == "ucdp"


if __name__ == "__main__":

    kg_schema = KGSchema(json.load(open('master_config.json')))
    etk = ETK(modules=[UCDPModule, UCDPActorModule], kg_schema=kg_schema)
    cp = CsvProcessor(etk=etk, heading_row=1)

    with open("ucdp.jl", "w") as f:
        for doc in cp.tabular_extractor(filename="ucdp_sample.xls", dataset='ucdp'):
            for result in etk.process_ems(doc):
                # print(result.cdr_document)
                f.write(json.dumps(result.cdr_document) + "\n")
