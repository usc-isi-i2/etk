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

class GTDModule(ETKModule):
    def __init__(self, etk):
        pass

    def  process_document(self, doc: Document) -> List[Document]:
        pass

    def document_selector(self, doc) -> bool:
        # return self.doc_selector.select_document(doc, datasets=["ucdp"])
        return doc.cdr_document.get("dataset") == "gtd"

if __name__ == "__main__":

    # Tell ETK the schema of the fields in the KG, the DIG master_config can be used as the schema.
    kg_schema = KGSchema(json.load(open('master_config.json')))

    # Instantiate ETK, with the two processing modules and the schema.
    etk = ETK(modules=[GTDModule], kg_schema=kg_schema)

    # Create a CSV processor to create documents for the relevant rows in the Excel sheet
    cp = CsvProcessor(etk=etk, heading_row=1)

    with open("gtd.jl", "w") as f:
        # Iterate over all the rows in the spredsheet
        for doc in cp.tabular_extractor(filename="globalterrorismdb_0617dist-nigeria.csv", dataset='gtd'):
            print(doc.value)
            exit(0)
            for result in etk.process_ems(doc):
                print(result.cdr_document["knowledge_graph"])
                f.write(json.dumps(result.cdr_document) + "\n")
