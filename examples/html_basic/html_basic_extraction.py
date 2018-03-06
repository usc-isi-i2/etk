import os, sys, json, codecs
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from etk.etk import ETK
from etk.extractors.html_content_extractor import HTMLContentExtractor, Strategy
from etk.extractors.html_metadata_extractor import HTMLMetadataExtractor
from etk.extractors.inferlink_extractor import InferlinkExtractor, InferlinkRuleSet


sample_html = json.load(codecs.open('sample_html.json', 'r')) # read sample file from disk

etk = ETK()
doc = etk.create_document(sample_html, mime_type="text/html", url="http://ex.com/123")

metadata_extractor = HTMLMetadataExtractor()
content_extractor = HTMLContentExtractor()
landmark_extractor = InferlinkExtractor(InferlinkRuleSet(InferlinkRuleSet.load_rules_file('sample_inferlink_rules.json')))

root = doc.select_segments("$")[0]

# root.store_extractions(doc.invoke_extractor(metadata_extractor, extract_title=True), "title")
# root.store_extractions(doc.invoke_extractor(metadata_extractor, extract_meta=True), "metadata")
root.store_extractions(doc.invoke_extractor(content_extractor, strategy=Strategy.ALL_TEXT), "etk2_text")
root.store_extractions(doc.invoke_extractor(content_extractor, strategy=Strategy.MAIN_CONTENT_STRICT), "etk2_content_strict")
root.store_extractions(doc.invoke_extractor(content_extractor, strategy=Strategy.MAIN_CONTENT_RELAXED), "etk2_content_relaxed")
root.store_extractions(doc.invoke_extractor(metadata_extractor), "etk2_metadata")

for e in doc.invoke_extractor(landmark_extractor):
    root.store_extractions([e], e.tag)

print(json.dumps(doc.cdr_document, indent=2))
