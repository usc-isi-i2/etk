import os, sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from etk.etk import ETK
from etk.extractors.spacy_rule_extractor import SpacyRuleExtractor
import json
from etk.etk_module import ETKModule


class RuleETKModule(ETKModule):
    """
    Abstract class for extraction module
    """
    def __init__(self, etk):
        ETKModule.__init__(self, etk)
        self.rule_extractor = SpacyRuleExtractor(self.etk.default_nlp, self.etk.load_spacy_rule("sample_rules.json"),
                                                 "test_extractor")

    def process_document(self, doc):
        """
        Add your code for processing the document
        """

        descriptions = doc.select_segments("projects[*].description")
        projects = doc.select_segments("projects[*]")

        for d, p in zip(descriptions, projects):
            names = doc.extract(self.rule_extractor, d)
            p.store(names, "members")
        return list()


if __name__ == "__main__":

    sample_input = {
        "projects": [
            {
                "name": "etk",
                "description": "version 2 of etk, implemented by Runqi12 Shao, Dongyu Li, Sylvia lin, Amandeep and others."
            },
            {
                "name": "rltk",
                "description": "record linkage toolkit, implemented by Pedro, Mayank, Yixiang and several students."
            }
        ]
    }

    etk = ETK(modules=RuleETKModule)
    doc = etk.create_document(sample_input)

    docs = etk.process_ems(doc)

    print(json.dumps(docs[0].value, indent=2))
