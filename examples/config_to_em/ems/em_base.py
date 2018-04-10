from etk.extraction_module import ExtractionModule
from etk.extractors.glossary_extractor import GlossaryExtractor
from etk.extractors.spacy_rule_extractor import SpacyRuleExtractor
from etk.extractors.html_content_extractor import HTMLContentExtractor, Strategy
from etk.document_selector import DefaultDocumentSelector
from etk.document import Document


class BaseExtractionModule(ExtractionModule):
    """
    Abstract class for extraction module
    """
    def __init__(self, etk):
        ExtractionModule.__init__(self, etk)
        self.readability_extractor = HTMLContentExtractor()
        self.country_extractor = GlossaryExtractor(self.etk.load_glossary('./glossaries/countries.txt'),
                                                   "country_extractor",
                                                   self.etk.default_tokenizer,
                                                   case_sensitive=False,
                                                   ngrams=3)

        self.phone_extractor = SpacyRuleExtractor(self.etk.default_nlp,
                                                  self.etk.load_spacy_rule("./spacy_rules/phone.json"),
                                                  "phone_extractor")

        self.state_extractor = GlossaryExtractor(self.etk.load_glossary('./glossaries/states_usa_canada.txt'),
                                                 "state_extractor",
                                                 self.etk.default_tokenizer,
                                                 case_sensitive=False,
                                                 ngrams=2)

        self.city_name_extractor = GlossaryExtractor(self.etk.load_glossary('./glossaries/cities.txt'),
                                                     "city_name_extractor",
                                                     self.etk.default_tokenizer,
                                                     case_sensitive=False,
                                                     ngrams=3)

        self.states_usa_codes_extractor = GlossaryExtractor(self.etk.load_glossary('./glossaries/states_usa_codes.txt'),
                                                            "states_usa_codes_extractor",
                                                            self.etk.default_tokenizer,
                                                            case_sensitive=False,
                                                            ngrams=1)

    def process_document(self, doc: Document):
        """
        Add your code for processing the document
        """
        doc.value["readability_extraction"] = {}

        all_text = self.readability_extractor.extract(doc.cdr_document["raw_content"], strategy=Strategy.ALL_TEXT)
        doc.select_segments("$.readability_extraction")[0].store_extractions(all_text, "all_text")

        strict_text = self.readability_extractor.extract(doc.cdr_document["raw_content"], strategy=Strategy.MAIN_CONTENT_STRICT)
        doc.select_segments("$.readability_extraction")[0].store_extractions(strict_text, "strict_text")

        relax_text = self.readability_extractor.extract(doc.cdr_document["raw_content"], strategy=Strategy.MAIN_CONTENT_RELAXED)
        doc.select_segments("$.readability_extraction")[0].store_extractions(relax_text, "relax_text")

        for text in all_text:
            for extraction in doc.invoke_extractor(self.country_extractor, text):
                doc.kg.add_value("country", extraction.value)
            for extraction in doc.invoke_extractor(self.phone_extractor, text):
                doc.kg.add_value("phone", extraction.value)
            for extraction in doc.invoke_extractor(self.state_extractor, text):
                doc.kg.add_value("state", extraction.value)
            for extraction in doc.invoke_extractor(self.city_name_extractor, text):
                doc.kg.add_value("city_name", extraction.value)
            for extraction in doc.invoke_extractor(self.states_usa_codes_extractor, text):
                doc.kg.add_value("states_usa_codes", extraction.value)

        for text in strict_text:
            for extraction in doc.invoke_extractor(self.country_extractor, text):
                doc.kg.add_value("country", extraction.value)
            for extraction in doc.invoke_extractor(self.phone_extractor, text):
                doc.kg.add_value("phone", extraction.value)
            for extraction in doc.invoke_extractor(self.state_extractor, text):
                doc.kg.add_value("state", extraction.value)
            for extraction in doc.invoke_extractor(self.city_name_extractor, text):
                doc.kg.add_value("city_name", extraction.value)
            for extraction in doc.invoke_extractor(self.states_usa_codes_extractor, text):
                doc.kg.add_value("states_usa_codes", extraction.value)

        for text in relax_text:
            for extraction in doc.invoke_extractor(self.country_extractor, text):
                doc.kg.add_value("country", extraction.value)
            for extraction in doc.invoke_extractor(self.phone_extractor, text):
                doc.kg.add_value("phone", extraction.value)
            for extraction in doc.invoke_extractor(self.state_extractor, text):
                doc.kg.add_value("state", extraction.value)
            for extraction in doc.invoke_extractor(self.city_name_extractor, text):
                doc.kg.add_value("city_name", extraction.value)
            for extraction in doc.invoke_extractor(self.states_usa_codes_extractor, text):
                doc.kg.add_value("states_usa_codes", extraction.value)

    def document_selector(self, doc: Document) -> bool:
        """
        Boolean function for selecting document
        Args:
            doc: Document

        Returns:

        """
        return DefaultDocumentSelector().select_document(doc)