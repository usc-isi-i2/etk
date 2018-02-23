import json
from etk2.etk import ETK
from etk2.extractors.glossary_extractor import GlossaryExtractor

sample_input = {
    "projects": [
        {
            "name": "etk",
            "description": "version 2 of etk, implemented by Runqi, Dongyu, Sylvia and others."
        }
    ]
}

etk = ETK()
doc = etk.create_document(sample_input)

name_extractor = GlossaryExtractor(etk.load_glossary("./names.txt"))

description_segments = doc.select_segments("projects[*].description")
names = doc.invoke_extractor(name_extractor, description_segments)

root_segment = doc.select_segments("$").items()[0]
root_segment.store_extractions(names, "all_names")

print(json.dumps(sample_input, indent=2))

