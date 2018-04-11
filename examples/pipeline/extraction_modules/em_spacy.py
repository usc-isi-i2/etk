from etk.extraction_module import ExtractionModule
from etk.document import Document
from etk.extractors.spacy_rule_extractor import SpacyRuleExtractor


class ExtractionModuleSpacy(ExtractionModule):
    def __init__(self, etk):
        ExtractionModule.__init__(self, etk)
        sample_rules = self.etk.load_spacy_rule("./extraction_modules/resources/sample_rules.json")

        self.sample_rule_extractor = SpacyRuleExtractor(self.etk.default_nlp, sample_rules, "test_extractor")

    def process_document(self, doc: Document):

        descriptions = doc.select_segments("projects[*].description")
        projects = doc.select_segments("projects[*]")

        for d, p in zip(descriptions, projects):
            spacy_names = doc.invoke_extractor(self.sample_rule_extractor, d)
            p.store_extractions(spacy_names, "spacy_names")
            for a_name in spacy_names:
                doc.kg.add_value("spacy_name", a_name.value)
