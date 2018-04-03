import os, sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from etk.etk import ETK
from etk.extractors.spacy_rule_extractor import SpacyRuleExtractor
import json

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

etk = ETK()
doc = etk.create_document(sample_input)

sample_rules = etk.load_spacy_rule("sample_rules.json")

sample_rule_extractor = SpacyRuleExtractor(etk.default_nlp, sample_rules, "test_extractor")

descriptions = doc.select_segments("projects[*].description")
projects = doc.select_segments("projects[*]")

for d, p in zip(descriptions, projects):
    names = doc.invoke_extractor(sample_rule_extractor, d)
    p.store_extractions(names, "members")

print(json.dumps(sample_input, indent=2))