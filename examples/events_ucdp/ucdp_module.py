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

        # Map location to country
        doc.kg.add_value("country", json_path="$.Location")

        # map incomp to type, after using a decoding dict
        doc.kg.add_value("type", value=doc.extract(self.incomp_decoder, doc.select_segments("$.Incomp")[0]))

        # map Int to type, also after using a decoding dict
        doc.kg.add_value("type", value=doc.extract(self.int_decoder, doc.select_segments("$.Int")[0]))

        # Add "Event" to type, as all these documents are events
        doc.kg.add_value("type", value="Event")

        # Add a title to our event
        doc.kg.add_value("title", value="{}/{} armed conflict in {}".format(
            doc.cdr_document["SideA"],
            doc.cdr_document["SideB"],
            doc.cdr_document["YEAR"]
        ))

        # Add the specific CauseEx ontology classes that we want to use for this event
        doc.kg.add_value("causeex_class", value=doc.extract(self.int_causeex_decoder, doc.select_segments("$.Int")[0]))
        doc.kg.add_value("causeex_class", value=self.event_prefix+"ArmedConflict")

        # Map dates to event_date
        doc.kg.add_value("event_date", json_path="$.StartDate")
        doc.kg.add_value("event_date_end", json_path="$.EpEndDate")

        # Create a CDR document for an actor, we only put the SideA attribute in it,
        # and we give it a new dataset identifier so we can match it in an ETKModule
        actor1_dict = {
            "Side": doc.cdr_document["SideA"],
            "dataset": "ucdp-actor"
        }
        actor1_doc = etk.create_document(actor1_dict)

        # Create a doc_id for the actor document, from the doc_id of the event document
        actor1_doc.doc_id = doc.doc_id + "_actor1"

        # Record the identifier of the actor object in the "actor" field of the event.
        doc.kg.add_value("actor", value=actor1_doc.doc_id)

        # Now do the exact same thing for SideB
        actor2_dict = {
            "Side": doc.cdr_document["SideB"],
            "dataset": "ucdp-actor"
        }
        actor2_doc = etk.create_document(actor2_dict)
        actor2_doc.doc_id = doc.doc_id + "_actor2"
        doc.kg.add_value("actor", value=actor2_doc.doc_id)

        # Create a fatalities object to record information about the fatalities in the conflict
        # Instead of creating an ETK module for it, it is possible to do it inline.
        fatalities_doc = etk.create_document({"Int": doc.cdr_document["Int"]})
        fatalities_doc.doc_id = doc.doc_id + "_fatalities"
        doc.kg.add_value("fatalities", value=fatalities_doc.doc_id)
        fatalities_doc.kg.add_value(
            "title",
            fatalities_doc.extract(self.int_fatalities_decoder, fatalities_doc.select_segments("$.Int")[0]))
        fatalities_doc.kg.add_value("type", value=["Group", "Dead People"])
        fatalities_doc.kg.add_value(
            "size_lower_bound", value=fatalities_doc.extract(self.int_fatalities_size_lower_decoder,
                                                             fatalities_doc.select_segments("$.Int")[0]))
        fatalities_doc.kg.add_value(
            "size_upper_bound", value=fatalities_doc.extract(self.int_fatalities_size_upper_decoder,
                                                             fatalities_doc.select_segments("$.Int")[0]))

        # Return the list of new documents that we created to be processed by ETK.
        # Note that fatalities_dco is in the list as it is a newly created document. It does not have an
        # extraction module, so it will be passed to the output unchanged.
        return [
            actor1_doc,
            actor2_doc,
            fatalities_doc
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
        return list()

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
                print(result.cdr_document["knowledge_graph"])
                f.write(json.dumps(result.cdr_document) + "\n")
