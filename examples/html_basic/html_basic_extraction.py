import json
import os, sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from etk.etk import ETK
from etk.extractors.html_content_extractor import HTMLContentExtractor
from etk.extractors.html_metadata_extractor import HTMLMetadataExtractor

sample_html = None # read sample file from disk

etk = ETK()
doc = etk.create_document(sample_html, content_type = "HTML")

metadata_extractor = HTMLMetadataExtractor()
content_extractor = HTMLContentExtractor()

descriptions_path = etk.parser("projects[*].description")
projects_path = etk.parser("projects[*]")

descriptions = doc.select_segments(descriptions_path)
projects = doc.select_segments(projects_path)

for d, p in zip(descriptions, projects):
    names = doc.invoke_extractor(name_extractor, d)
    p.store_extractions(names, "members")

print(json.dumps(sample_input, indent=2))
