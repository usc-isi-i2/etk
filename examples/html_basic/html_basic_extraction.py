import os, sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

import json
from etk.etk import ETK
from etk.extractors.html_content_extractor import HTMLContentExtractor
from etk.extractors.html_metadata_extractor import HTMLMetadataExtractor


sample_html = None  # read sample file from disk

etk = ETK()
doc = etk.create_document(sample_html, mime_type="text/html", url="http://ex.com/123")

metadata_extractor = HTMLMetadataExtractor()
content_extractor = HTMLContentExtractor()

root = doc.select_segments(etk.parser("$"))[0]

# Passing arguments to extractors using keyword arguments in invoke_extractor is causing warnings.
# Is there a pythonic way to do this?
root.store_extractions(doc.invoke_extractor(metadata_extractor, extract_title=True), "title")
root.store_extractions(doc.invoke_extractor(metadata_extractor, extract_meta=True), "metadata")
root.store_extractions(doc.invoke_extractor(content_extractor, stategy=HTMLContentExtractor.Strategy.ALL_TEXT), "text")

print(json.dumps(doc.cdr_document, indent=2))
