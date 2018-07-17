import os, sys, json, codecs
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from etk.etk import ETK
from etk.extractors.inferlink_extractor import InferlinkExtractor, InferlinkRuleSet
from etk.etk_module import ETKModule


class InferlinkETKModule(ETKModule):
    """
    Abstract class for extraction module
    """
    def __init__(self, etk):
        ETKModule.__init__(self, etk)
        self.inferlink_extractor = InferlinkExtractor(
            InferlinkRuleSet(InferlinkRuleSet.load_rules_file('../html_basic/sample_inferlink_rules.json')))

    def process_document(self, doc):
        """
        Add your code for processing the document
        """

        raw = doc.select_segments("$.raw_content")[0]
        extractions = doc.extract(self.inferlink_extractor, raw)
        doc.store(extractions, "inferlink_extraction")
        return list()


if __name__ == "__main__":
    sample_html = json.load(codecs.open('../html_basic/sample_html.json', 'r')) # read sample file from disk

    etk = ETK(modules=InferlinkETKModule)
    doc = etk.create_document(sample_html, mime_type="text/html", url="http://ex.com/123")

    docs = etk.process_ems(doc)

    print(json.dumps(docs[0].value, indent=2))