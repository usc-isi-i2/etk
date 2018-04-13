import os, sys, json, codecs
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from etk.etk import ETK
from etk.extractors.html_content_extractor import HTMLContentExtractor, Strategy
from etk.extractors.html_metadata_extractor import HTMLMetadataExtractor
from etk.extraction_module import ExtractionModule


class HtmlBasicExtractionModule(ExtractionModule):
    """
    Abstract class for extraction module
    """
    def __init__(self, etk):
        ExtractionModule.__init__(self, etk)
        self.metadata_extractor = HTMLMetadataExtractor()
        self.content_extractor = HTMLContentExtractor()

    def process_document(self, doc):
        """
        Add your code for processing the document
        """

        raw = doc.select_segments("$.raw_content")[0]

        doc.store_extractions(doc.invoke_extractor(self.content_extractor, raw, strategy=Strategy.ALL_TEXT), "etk2_text")
        doc.store_extractions(doc.invoke_extractor(self.content_extractor, raw, strategy=Strategy.MAIN_CONTENT_STRICT),
                              "etk2_content_strict")
        doc.store_extractions(doc.invoke_extractor(self.content_extractor, raw, strategy=Strategy.MAIN_CONTENT_RELAXED),
                              "etk2_content_relaxed")
        doc.store_extractions(doc.invoke_extractor(self.metadata_extractor, raw), "etk2_metadata")


if __name__ == "__main__":

    sample_html = json.load(codecs.open('sample_html.json', 'r')) # read sample file from disk

    etk = ETK(modules=HtmlBasicExtractionModule)
    doc = etk.create_document(sample_html, mime_type="text/html", url="http://ex.com/123")

    doc, _ = etk.process_ems(doc)

    print(json.dumps(doc.value, indent=2))
