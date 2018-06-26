import os, sys, json, codecs
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from etk.etk import ETK
from etk.extractors.html_content_extractor import HTMLContentExtractor, Strategy
from etk.extractors.html_metadata_extractor import HTMLMetadataExtractor
from etk.etk_module import ETKModule


class HtmlBasicETKModule(ETKModule):
    """
    Abstract class for extraction module
    """
    def __init__(self, etk):
        ETKModule.__init__(self, etk)
        self.metadata_extractor = HTMLMetadataExtractor()
        self.content_extractor = HTMLContentExtractor()

    def process_document(self, doc):
        """
        Add your code for processing the document
        """

        raw = doc.select_segments("$.raw_content")[0]

        doc.store(doc.extract(self.content_extractor, raw, strategy=Strategy.ALL_TEXT), "etk2_text")
        doc.store(doc.extract(self.content_extractor, raw, strategy=Strategy.MAIN_CONTENT_STRICT),
                              "etk2_content_strict")
        doc.store(doc.extract(self.content_extractor, raw, strategy=Strategy.MAIN_CONTENT_RELAXED),
                              "etk2_content_relaxed")
        doc.store(doc.extract(self.metadata_extractor,
                              raw,
                              extract_title=True,
                              extract_meta=True,
                              extract_microdata=True,
                              extract_rdfa=True,
                              ), "etk2_metadata")
        return list()


if __name__ == "__main__":

    sample_html = json.load(codecs.open('sample_html.json', 'r')) # read sample file from disk

    etk = ETK(modules=HtmlBasicETKModule)
    doc = etk.create_document(sample_html, mime_type="text/html", url="http://ex.com/123")

    docs= etk.process_ems(doc)

    print(json.dumps(docs[0].value, indent=2))
