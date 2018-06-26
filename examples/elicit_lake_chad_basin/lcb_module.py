import os
import sys, json

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from typing import List
from etk.etk import ETK
from etk.etk_module import ETKModule
from etk.csv_processor import CsvProcessor
from etk.document import Document
from etk.knowledge_graph_schema import KGSchema
from etk.utilities import Utility
from etk.extractors.date_extractor import DateExtractor


class LakeChadBasinDisplacedModule(ETKModule):

    def __init__(self, etk):
        ETKModule.__init__(self, etk)
        self.date_extractor = DateExtractor(self.etk, 'lcb_date_parser')

    def process_document(self, doc: Document) -> List[Document]:
        nested_docs = list()
        doc.doc_id = Utility.create_doc_id_from_json(doc.cdr_document)

        doc.cdr_document["title"] = "{Total} Displaced from {ReportedLocation} in {Country}".format(
            Total=doc.cdr_document["Total"],
            ReportedLocation=doc.cdr_document["ReportedLocation"],
            Country=doc.cdr_document["Country"]
        )
        doc.cdr_document["dataset"] = "lake_chad_basin_displaced"

        place = {
            "uri": '{}_place'.format(doc.doc_id),
            "doc_id": '{}_place'.format(doc.doc_id),
            "country": doc.cdr_document.get("Country", ''),
            "dataset": "lcb_place"
        }
        place_doc = etk.create_document(place)
        nested_docs.append(place_doc)
        doc.kg.add_value("place", value='{}_place'.format(doc.doc_id))

        # Add event_date to the KG
        extracted_dates = self.date_extractor.extract(doc.cdr_document.get('Period', ''))
        doc.kg.add_value("event_date", value=extracted_dates)
        doc.kg.add_value("event_date_end", value=extracted_dates)

        doc.kg.add_value("location", json_path="ReportedLocation")
        doc.kg.add_value("causeex_class", value="http://ontology.causeex.com/ontology/odps/EventHierarchy#ForcedMove")
        doc.kg.add_value("type", value=["event", "Displacement Event"])
        doc.kg.add_value("title", json_path="title")

        victim = {
            "dataset": "lake_chad_basin_displaced_victim",
            "total": doc.cdr_document["Total"],
            "type": ["Group", "Displaced People"],
            "uri": '{}_victim'.format(doc.doc_id)
        }
        victim_doc = etk.create_document(victim)
        victim_doc.doc_id = '{}_victim'.format(doc.doc_id)

        doc.kg.add_value("victim", value=victim_doc.doc_id)
        nested_docs.append(victim_doc)

        return nested_docs

    def document_selector(self, doc) -> bool:
        return doc.cdr_document.get("dataset") == "lake_chad_basin_displaced"


class LCBPlaceModule(ETKModule):
    """
        ETK module to process Lake Chad Basin Place documents
        """

    def __init__(self, etk):
        ETKModule.__init__(self, etk)

    def document_selector(self, doc) -> bool:
        return doc.cdr_document.get("dataset") == "lcb_place"

    def process_document(self, doc: Document) -> List[Document]:
        doc.kg.add_value("type", value="Place")
        doc.kg.add_value("country", json_path="$.country")

        return list()


class LakeChadBasinDisplacedVictimModule(ETKModule):

    def __init__(self, etk):
        ETKModule.__init__(self, etk)

    def document_selector(self, doc) -> bool:
        return doc.cdr_document.get("dataset") == "lake_chad_basin_displaced_victim"

    def process_document(self, doc: Document) -> List[Document]:
        doc.kg.add_value("size", json_path="total")
        doc.kg.add_value("type", json_path="type")
        return list()


if __name__ == "__main__":
    dir_path = sys.argv[1]
    master_config_path = sys.argv[2]
    file_name = 'lake_chad_basin_displaced.csv'
    input_path = os.path.join(dir_path, file_name)
    output_path = os.path.join(dir_path, file_name + '.jl')

    kg_schema = KGSchema(json.load(open(master_config_path)))
    etk = ETK(modules=[LakeChadBasinDisplacedModule, LakeChadBasinDisplacedVictimModule, LCBPlaceModule],
              kg_schema=kg_schema)
    cp = CsvProcessor(etk=etk, heading_row=1, content_start_row=3)

    with open(output_path, "w") as f:
        print(input_path, output_path)
        for doc in cp.tabular_extractor(filename=input_path, dataset='lake_chad_basin_displaced'):
            etk.process_and_frame(doc)
            f.write(json.dumps(doc.cdr_document) + "\n")
