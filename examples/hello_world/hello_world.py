import json
import os, sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from etk.etk import ETK
from etk.extractors.glossary_extractor import GlossaryExtractor

sample_input = {
    "projects": [
        {
            "name": "etk",
            "description": "version 2 of etk, implemented by Runqi, Dongyu, Sylvia, Amandeep and others."
        },
        {
            "name": "rltk",
            "description": "record linkage toolkit, implemented by Pedro, Mayank, Yixiang and several students."
        }
    ]
}

etk = ETK()
doc = etk.create_document(sample_input)

name_extractor = GlossaryExtractor(etk.load_glossary("./names.txt"), "name_extractor", etk.default_tokenizer, case_sensitive=False, ngrams=1)

# TODO: Pedro thinks this is ugly, and we should allow users to pass in strings to select_segments
descriptions_path = etk.parser("projects[*].description")
projects_path = etk.parser("projects[*]")

descriptions = doc.select_segments(descriptions_path)
projects = doc.select_segments(projects_path)

for d, p in zip(descriptions, projects):
    names = doc.invoke_extractor(name_extractor, d)
    p.store_extractions(names, "members")

print(json.dumps(sample_input, indent=2))
