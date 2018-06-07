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


class UCDPModule(ETKModule):
    """
    ETK module to process event documents
    """

    event_prefix = "http://ontology.causeex.com/ontology/odps/Event#"

    incomp_type = {
        "1": "Territory Conflict",
        "2": "Government Conflict",
        "3": "Government and Territory Conflict",
    }

    int_event_type = {
        "1": "Minor Conflict",
        "2": "War"
    }

    int_fatalities = {
        "1": "Between 25 and 999 battle-related deaths in a given year",
        "2": "At least 1,000 battle-related deaths in a given year"
    }

    int_fatalities_size_lower = {
        "1": 25,
        "2": 1000
    }

    int_fatalities_size_upper = {
        "1": 999
    }

    int_causeex_type = {
        "1": event_prefix + "War"
    }

    def __init__(self, etk):
        ETKModule.__init__(self, etk)
        self.doc_selector = DefaultDocumentSelector()
        self.incomp_decoder = DecodingValueExtractor(self.incomp_type, 'Incomp Decoder')
        self.int_decoder = DecodingValueExtractor(self.int_event_type, 'Int Decoder')
        self.int_fatalities_decoder = DecodingValueExtractor(self.int_fatalities, 'Int Fatalities Decoder')
        self.int_fatalities_size_lower_decoder = DecodingValueExtractor(self.int_fatalities_size_lower,
                                                                        'Int Fatalities Lower Bound Size Decoder')
        self.int_fatalities_size_upper_decoder = DecodingValueExtractor(self.int_fatalities_size_upper,
                                                                        'Int Fatalities Upper Bound Size Decoder',
                                                                        default_action="delete")
        self.int_causeex_decoder = DecodingValueExtractor(self.int_causeex_type,
                                                          'Int CauseEx Type',
                                                          default_action="delete")

    def process_document(self, doc: Document) -> List[Document]:
        # pyexcel produces dics with date objects, which are not JSON serializable, fix that.
        Utility.make_json_serializable(doc.cdr_document)

        # Add an ID based on the full contents of the raw document
        doc.doc_id = Utility.create_doc_id_from_json(doc.cdr_document)

        # Create a CDR document for an actor, we only put the SideA attribute in it,
        # and we give it a new dataset identifier so we can match it in an ETKModule
        actor1_dict = {
            "Side": doc.cdr_document["SideA"],
            "dataset": "ucdp-actor"
        }
        actor1_doc = etk.create_document(actor1_dict)

        # Create a doc_id for the actor document, from the doc_id of the event document
        actor1_doc.doc_id = doc.doc_id + "_actor1"

        # Now do the exact same thing for SideB
        actor2_dict = {
            "Side": doc.cdr_document["SideB"],
            "dataset": "ucdp-actor"
        }
        actor2_doc = etk.create_document(actor2_dict)
        actor2_doc.doc_id = doc.doc_id + "_actor2"

        kg_object_old_ontology = {
            "uri": doc.doc_id,
            "country": doc.select_segments("$.Location"),
            "type": [
                "Event",
                doc.extract(self.incomp_decoder, doc.select_segments("$.Incomp")[0]),
                doc.extract(self.int_decoder, doc.select_segments("$.Int")[0])
            ],
            "title": "{}/{} armed conflict in {}".format(
                doc.cdr_document["SideA"],
                doc.cdr_document["SideB"],
                doc.cdr_document["YEAR"]
            ),
            "causeex_class": [
                doc.extract(self.int_causeex_decoder, doc.select_segments("$.Int")[0]),
                self.event_prefix + "ArmedConflict"
            ],
            "event_date": doc.select_segments("$.StartDate"),
            "event_date_end": doc.select_segments("$.EpEndDate"),
            "fatalities": {
                "uri": doc.doc_id + "_fatalities",
                "title": doc.extract(self.int_fatalities_decoder, doc.select_segments("$.Int")[0]),
                "type": ["Group", "Dead People"],
                "size_lower_bound": doc.extract(self.int_fatalities_size_lower_decoder,
                                                doc.select_segments("$.Int")[0]),
                "size_upper_bound": doc.extract(self.int_fatalities_size_upper_decoder, doc.select_segments("$.Int")[0])
            },
            "actor": [actor1_doc.doc_id, actor2_doc.doc_id]
        }
        ds = doc.build_knowledge_graph(kg_object_old_ontology)
        # print(len(ds))
        # print(doc.kg._kg['type'])
        # print(doc.cdr_document['knowledge_graph'])
        for d in ds:
            print(d.kg._kg)
            # if 'fatalities' in d.kg._kg:
            #     print(d.kg._kg['fatalities'])
            #     print(d.cdr_document)
            # print(d.cdr_document['knowledge_graph'])

        kg_object_new_ontology = {
            "a": "Event",
            "uri": doc.doc_id,
            "took_place_at": {
                "country": doc.select_segments("$.Location")
            },
            "type": [
                doc.extract(self.incomp_decoder, doc.select_segments("$.Incomp")[0]),
                doc.extract(self.int_decoder, doc.select_segments("$.Int")[0])
            ],
            "title": "{}/{} armed conflict in {}".format(
                doc.cdr_document["SideA"],
                doc.cdr_document["SideB"],
                doc.cdr_document["YEAR"]
            ),
            "causeex_class": [
                doc.extract(self.int_causeex_decoder, doc.select_segments("$.Int")[0]),
                self.event_prefix + "ArmedConflict"
            ],
            "has_time_span": {
                "start_date": doc.select_segments("$.StartDate"),
                "end_date": doc.select_segments("$.EpEndDate")
            },
            "had_victim": {
                "a": "Group",
                "has_dimension": {
                    "type": "size",
                    "has_unit": "person",
                    "value_is_at_least": doc.extract(self.int_fatalities_size_lower_decoder,
                                                     doc.select_segments("$.Int")[0]),
                    "value_is_at_most": doc.extract(self.int_fatalities_size_upper_decoder,
                                                    doc.select_segments("$.Int")[0])
                },
                "has_condition": {
                    "type": "Dead"
                }
            },
            "carried_out_by": [actor1_doc.doc_id, actor2_doc.doc_id]
        }

        # Return the list of new documents that we created to be processed by ETK.
        # Note that fatalities_dco is in the list as it is a newly created document. It does not have an
        # extraction module, so it will be passed to the output unchanged.
        return [
            actor1_doc,
            actor2_doc
        ]

    def document_selector(self, doc) -> bool:
        # return self.doc_selector.select_document(doc, datasets=["ucdp"])
        return doc.cdr_document.get("dataset") == "ucdp"


class UCDPActorModule(ETKModule):
    """
    ETK module to process Actor documents
    """

    def __init__(self, etk):
        ETKModule.__init__(self, etk)

    def document_selector(self, doc) -> bool:
        return doc.cdr_document.get("dataset") == "ucdp-actor"

    def process_document(self, doc: Document) -> List[Document]:
        # Record the type of the actor
        doc.kg.add_value("type", value=["Group", "Country"])

        # Record the country of this actor
        doc.kg.add_value("country", json_path="$.Side")

        # Add a title to the actor document
        doc.kg.add_value("title", json_path="$.Side")

        # Return an empty list because we didn't create new documents
        return []


# The main is for testing, and is not used in the DIG pipeline
if __name__ == "__main__":

    # Tell ETK the schema of the fields in the KG, the DIG master_config can be used as the schema.
    kg_schema = KGSchema(json.load(open('master_config.json')))

    # Instantiate ETK, with the two processing modules and the schema.
    etk = ETK(modules=[UCDPModule, UCDPActorModule], kg_schema=kg_schema)

    # Create a CSV processor to create documents for the relevant rows in the Excel sheet
    cp = CsvProcessor(etk=etk, heading_row=1)

    with open("ucdp.jl", "w") as f:
        # Iterate over all the rows in the spredsheet
        for doc in cp.tabular_extractor(filename="ucdp_sample.xls", dataset='ucdp'):
            # Each row produces a document, which we sent to ETK.
            # Note that each invocation of process_ems will also process any new documents created while
            # processing each doc
            for result in etk.process_ems(doc):
                # print(result.cdr_document["knowledge_graph"])
                f.write(json.dumps(result.cdr_document) + "\n")
