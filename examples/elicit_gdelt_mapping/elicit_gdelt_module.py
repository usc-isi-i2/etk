import os
import sys, json
from typing import List
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from etk.etk import ETK
from etk.etk_module import ETKModule
from etk.csv_processor import CsvProcessor
from etk.extractors.date_extractor import DateExtractor
from etk.extractors.decoding_value_extractor import DecodingValueExtractor
from etk.document_selector import DefaultDocumentSelector
from etk.document import Document
from etk.knowledge_graph_schema import KGSchema
from etk.utilities import Utility
from examples.elicit_gdelt_mapping.elicit_gdelt_api import GdeltMapping


class GdeltModule(ETKModule):
    """
    ETK module to process gdelt event documents
    """

    header_fields = [
        "GLOBALEVENTID",
        "SQLDATE",
        "MonthYear",
        "Year",
        "FractionDate",
        "Actor1Code",
        "Actor1Name",
        "Actor1CountryCode",
        "Actor1KnownGroupCode",
        "Actor1EthnicCode",
        "Actor1Religion1Code",
        "Actor1Religion2Code",
        "Actor1Type1Code",
        "Actor1Type2Code",
        "Actor1Type3Code",
        "Actor2Code",
        "Actor2Name",
        "Actor2CountryCode",
        "Actor2KnownGroupCode",
        "Actor2EthnicCode",
        "Actor2Religion1Code",
        "Actor2Religion2Code",
        "Actor2Type1Code",
        "Actor2Type2Code",
        "Actor2Type3Code",
        "IsRootEvent",
        "EventCode",
        "EventBaseCode",
        "EventRootCode",
        "QuadClass",
        "GoldsteinScale",
        "NumMentions",
        "NumSources",
        "NumArticles",
        "AvgTone",
        "Actor1Geo_Type",
        "Actor1Geo_FullName",
        "Actor1Geo_CountryCode",
        "Actor1Geo_ADM1Code",
        "Actor1Geo_Lat",
        "Actor1Geo_Long",
        "Actor1Geo_FeatureID",
        "Actor2Geo_Type",
        "Actor2Geo_FullName",
        "Actor2Geo_CountryCode",
        "Actor2Geo_ADM1Code",
        "Actor2Geo_Lat",
        "Actor2Geo_Long",
        "Actor2Geo_FeatureID",
        "ActionGeo_Type",
        "ActionGeo_FullName",
        "ActionGeo_CountryCode",
        "ActionGeo_ADM1Code",
        "ActionGeo_Lat",
        "ActionGeo_Long",
        "ActionGeo_FeatureID",
        "DATEADDED",
        "SOURCEURL"
    ]

    ontology_prefixes = {
        "event": "http://ontology.causeex.com/ontology/odps/EventHierarchy#"
    }

    header_translation_table = {}

    def __init__(self, etk):
        ETKModule.__init__(self, etk)
        self.mapping = GdeltMapping(json.load(open("ODP-Mappings-V3.1.json")))
        # As our input files have no header, create a translation table to go from names to indices.
        for i in range(0, len(self.header_fields)):
            self.header_translation_table[self.header_fields[i]] = "COL" + str(i)
        # Extractors
        self.date_extractor = DateExtractor(self.etk, "Date Extractor")

    def attribute(self, attribute_name: str):
        """
        Translate an attribute_name to the key in the JSON
        Args:
            attribute_name: attribute names as given in the code book

        Returns: the key in the JSON document

        """
        return self.header_translation_table[attribute_name]

    def attribute_value(self, doc: Document, attribute_name: str):
        """
        Access data using attribute name rather than the numeric indices

        Returns: the value for the attribute

        """
        return doc.cdr_document.get(self.header_translation_table[attribute_name])

    def expand_prefix(self, uri: str):
        prefix, name = uri.split(":")
        if prefix and name:
            return self.ontology_prefixes.get(prefix) + name
        else:
            return None

    def document_selector(self, doc: Document) -> bool:
        return doc.cdr_document.get("dataset") == "gdelt"

    def process_document(self, doc: Document) -> List[Document]:
        cameo_code = self.attribute_value(doc, "EventCode")

        doc.kg.add_value("type", "Event")
        if self.mapping.has_cameo_code(cameo_code):
            # Add type fields
            for t in self.mapping.event_type("event1", cameo_code):
                doc.kg.add_value("type", t)
                doc.kg.add_value("causeex_class", self.expand_prefix(t))

            # Add event_date
            for s in doc.select_segments("$." + self.attribute("SQLDATE")):
                doc.kg.add_value("event_date", doc.extract(self.date_extractor, s,
                                                           prefer_language_date_order=None,
                                                           additional_formats=["%Y%m%d"],
                                                           use_default_formats=False))

        return []

if __name__ == "__main__":

    # Tell ETK the schema of the fields in the KG, the DIG master_config can be used as the schema.
    kg_schema = KGSchema(json.load(open('../events_ucdp/master_config.json')))

    # Instantiate ETK, with the two processing modules and the schema.
    etk = ETK(modules=[GdeltModule], kg_schema=kg_schema)

    # Create a CSV processor to create documents for the relevant rows in the TSV file
    cp = CsvProcessor(etk=etk,
                      heading_columns=(1, len(GdeltModule.header_fields)),
                      column_name_prefix="COL")

    with open("gdelt.jl", "w") as f:
        # Iterate over all the rows in the spredsheet
        for d in cp.tabular_extractor(filename="20170912.export_sample.tsv", dataset='gdelt'):
            for result in etk.process_ems(d):
                print(d.cdr_document)
                print(result.cdr_document["knowledge_graph"])
                f.write(json.dumps(result.cdr_document) + "\n")
