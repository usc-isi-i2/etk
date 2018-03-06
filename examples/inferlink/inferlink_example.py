import os, sys, json, codecs
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from etk.etk import ETK
from etk.document import Document
from etk.extractors.inferlink_extractor import InferlinkExtractor, InferlinkRuleSet


sample_html = json.load(codecs.open('../html_basic/sample_html.json', 'r')) # read sample file from disk


etk = ETK()
doc = etk.create_document(sample_html, mime_type="text/html", url="http://ex.com/123")

rule_set = InferlinkRuleSet.load_rules_file('sample_inferlink_rules.json')

inferlink_extractor = InferlinkExtractor(InferlinkRuleSet(rule_set))

extractions = doc.invoke_extractor(inferlink_extractor)

# create a an empty dict in the cdr document to hold the inferlink extractions
doc.cdr_document["my_location_for_inferlink"] = {}

# bind the new location to a segment
target = doc.select_segments("$.my_location_for_inferlink")[0]

# Store the extractions in the target segment.
# note: given that we allow users to get cdr_document, they could bypass the segments
# and store the extractions directly where they want. This would work, but ETK will not
# be able to record the provenance.
for e in extractions:
    target.store_extractions([e], e.tag)


# We can make the cdr_document hidden, provide a Segment.add_segment function, and then
# the user would define the target as follows:
# --- not sure what this means : ---
# target = doc.select_segments("$")[0].add_segmment("my_location_for_inferlink")

print(json.dumps(target.value, indent=2))