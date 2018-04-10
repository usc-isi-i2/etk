import os, sys, json, codecs
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from etk.etk import ETK
from etk.extractors.inferlink_extractor import InferlinkExtractor, InferlinkRuleSet
from etk.extraction_module import ExtractionModule


class InferlinkExtractionModule(ExtractionModule):
    """
    Abstract class for extraction module
    """
    def __init__(self, etk):
        ExtractionModule.__init__(self, etk)
        self.inferlink_extractor = InferlinkExtractor(
            InferlinkRuleSet(InferlinkRuleSet.load_rules_file('../html_basic/sample_inferlink_rules.json')))

    def process_document(self, doc):
        """
        Add your code for processing the document
        """

        raw = doc.select_segments("$.raw_content")[0]
        extractions = doc.invoke_extractor(self.inferlink_extractor, raw)

        # bind the new location to a segment
        # Store the extractions in the target segment.
        # note: given that we allow users to get cdr_document, they could bypass the segments
        # and store the extractions directly where they want. This would work, but ETK will not
        # be able to record the provenance.
        doc.store_extractions(extractions, "my_location_for_inferlink")

        # We can make the cdr_document hidden, provide a Segment.add_segment function, and then
        # the user would define the target as follows:
        # --- not sure what this means : ---
        # target = doc.select_segments("$")[0].add_segmment("my_location_for_inferlink")


if __name__ == "__main__":
    sample_html = json.load(codecs.open('../html_basic/sample_html.json', 'r')) # read sample file from disk

    etk = ETK(modules=InferlinkExtractionModule)
    doc = etk.create_document(sample_html, mime_type="text/html", url="http://ex.com/123")

    doc, _ = etk.process_ems(doc)

    print(json.dumps(doc.value, indent=2))
