from etk.etk import ETK
from etk.etk_module import ETKModule
from etk.csv_processor import CsvProcessor
from etk.extractors.date_extractor import DateExtractor
from etk.document import Document
from etk.extractors.glossary_extractor import GlossaryExtractor
from etk.extractors.decoding_value_extractor import DecodingValueExtractor


class AcledModule(ETKModule):

    def __init__(self, etk):
        ETKModule.__init__(self, etk)
        self.date_extractor = DateExtractor(self.etk, 'acled_date_parser')
        self.country_extractor = GlossaryExtractor(
            self.etk.load_glossary(
                "${GLOSSARY_PATH}/countries.json.gz", read_json=True),
            "country_extractor",
            self.etk.default_tokenizer,
            case_sensitive=False, ngrams=3)
        self.states_extractor = GlossaryExtractor(
            self.etk.load_glossary(
                "${GLOSSARY_PATH}/states_usa_canada.json.gz", read_json=True),
            "states_extractor",
            self.etk.default_tokenizer,
            case_sensitive=False, ngrams=3)
        self.cities_extractor = GlossaryExtractor(
            self.etk.load_glossary(
                "${GLOSSARY_PATH}/cities.json.gz", read_json=True),
            "cities_extractor",
            self.etk.default_tokenizer,
            case_sensitive=False, ngrams=3)
        self.csv_processor = CsvProcessor(etk=etk, heading_row=1)
        self.interaction_decoding_dict = {
            "10": "Sole Military Action",
            "11": "Military Versus Military",
            "12": "Military Versus Rebels",
            "13": "Military Versus Political Militia",
            "14": "Military Versus Communal Militia",
            "15": "Military Versus Rioters",
            "16": "Military Versus Protesters",
            "17": "Military Versus Civilians",
            "18": "Military Versus Other",
            "20": "Sole Rebel Action",
            "22": "Rebels Versus Rebels",
            "23": "Rebels Versus Political Militia",
            "24": "Rebels Versus Communal Militia",
            "25": "Rebels Versus Rioters",
            "26": "Rebels Versus Protesters",
            "27": "Rebels Versus Civilians",
            "28": "Rebels Versus Other",
            "30": "Sole Political Militia Action",
            "33": "Political Militia Versus Political Militia",
            "34": "Political Militia Versus Communal Militia",
            "35": "Political Militia Versus Rioters",
            "36": "Political Militia Versus Protesters",
            "37": "Political Militia Versus Civilians",
            "38": "Political Militia Versus Other",
            "40": "Sole Communal Militia Action",
            "44": "Communal Militia Versus Communal Militia",
            "45": "Communal Militia Versus Rioters",
            "46": "Communal Militia Versus Protesters",
            "47": "Communal Militia Versus Civilians",
            "48": "Communal Militia Versus Other",
            "50": "Sole Rioter Action",
            "55": "Rioters Versus Rioters",
            "56": "Rioters Versus Protesters",
            "57": "Rioters Versus Civilians",
            "58": "Rioters Versus Other",
            "60": "Sole Protester Action",
            "66": "Protesters Versus Protesters",
            "68": "Protesters Versus Other",
            "78": "Other Actor Versus Civilians",
            "80": "Sole Other Action"
        }
        self.interaction_decoder = DecodingValueExtractor(self.interaction_decoding_dict, 'default_decoding',
                                                          case_sensitive=True)

    def process_document(self, cdr_doc: Document):
        new_docs = list()

        cdr_doc_json = cdr_doc.cdr_document
        if 'raw_content_path' in cdr_doc_json and cdr_doc_json['raw_content_path'].strip() != '':
            try:
                docs = self.csv_processor.tabular_extractor(filename=cdr_doc_json['raw_content_path'],
                                                            dataset='acleddata',
                                                            doc_id_field="data_id")
                for doc in docs:
                    doc_json = doc.cdr_document
                    event_date = doc.select_segments(jsonpath='$.event_date')
                    for segment in event_date:
                        extractions = doc.extract(
                            extractor=self.date_extractor, extractable=segment)

                        for extraction in extractions:
                            doc.kg.add_value(
                                "event_date", value=extraction.value)

                    doc.kg.add_value("website", value='acleddata.com')
                    doc.kg.add_value("description", json_path='$.notes')
                    acled_title = "{event_date}: {event_type} in {location}".format(
                        event_date=doc.cdr_document.get("event_date", ''),
                        event_type=doc.cdr_document.get("event_type", ''),
                        location=doc.cdr_document.get("location", ''))
                    doc.kg.add_value("title", value=acled_title)

                    doc.kg.add_value('country', json_path="$.country")

                    states_segments = doc.select_segments("$.state")
                    for state_segment in states_segments:
                        extracted_states = doc.extract(
                            self.states_extractor, state_segment)
                        doc.kg.add_value("state", value=extracted_states)

                    interaction_segments = doc.select_segments("$.interaction")
                    for interaction_segment in interaction_segments:
                        extracted_interaction = doc.extract(
                            self.interaction_decoder, interaction_segment)
                        doc.kg.add_value("type", value=extracted_interaction)

                    doc.kg.add_value("type", json_path="$.event_type")
                    doc.kg.add_value("type", json_path="$.type")
                    doc.kg.add_value("type", value="Event")

                    location_segments = doc.select_segments("$.location")
                    for location_segment in location_segments:
                        ec_location = doc.extract(
                            self.country_extractor, location_segment)
                        doc.kg.add_value("country", ec_location)

                        ecity_location = doc.extract(
                            self.cities_extractor, location_segment)
                        doc.kg.add_value("city_name", ecity_location)

                        es_location = doc.extract(
                            self.states_extractor, location_segment)
                        doc.kg.add_value("state", es_location)


                    new_docs.append(doc)
            except Exception as e:
                raise Exception('Error in AcledModule', e)
        return new_docs

    def document_selector(self, doc) -> bool:
        """
        Boolean function for selecting document
        Args:
            doc: Document

        Returns:

        """
        return doc.cdr_document.get("dataset") == "acleddata"


