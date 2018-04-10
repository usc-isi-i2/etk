import os, sys, json, codecs
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from etk.etk import ETK
from etk.extractors.table_extractor import TableExtractor, EntityTableDataExtraction
from etk.extraction_module import ExtractionModule


class TableExtractionModule(ExtractionModule):
    """
    Abstract class for extraction module
    """
    def __init__(self, etk):
        ExtractionModule.__init__(self, etk)
        self.table_extractor = TableExtractor()

    def process_document(self, doc):
        """
        Add your code for processing the document
        """
        d = doc.select_segments("$.raw_content")[0]

        tables = doc.invoke_extractor(self.table_extractor, d)
        for t in tables:
            doc.store_extractions([t], t.tag, group_by_tags=False)

        table_data_extractor = EntityTableDataExtraction()
        table_data_extractor.add_glossary(etk.load_glossary("./resources/address_dict.txt"), "address")
        table_data_extractor.add_glossary(etk.load_glossary("./resources/calibre_dict.txt"), "caliber")
        table_data_extractor.add_glossary(etk.load_glossary("./resources/capacity_dict.txt"), "capacity")
        table_data_extractor.add_glossary(etk.load_glossary("./resources/manufacturer_dict.txt"), "manufacturer")
        table_data_extractor.add_glossary(etk.load_glossary("./resources/price_dict.txt"), "price")

        tables = doc.select_segments("$.tables[*]")

        for t in tables:
            extractions = doc.invoke_extractor(table_data_extractor, t)
            doc.store_extractions(extractions, "table_data_extraction")


if __name__ == "__main__":

    sample_html = json.load(codecs.open('./table_data.json', 'r')) # read sample file from disk

    etk = ETK(modules=TableExtractionModule)

    doc = etk.create_document(sample_html, mime_type="text/html", url="http://ex.com/123")
    doc, _ = etk.process_ems(doc)

    print(json.dumps(doc.value, indent=2))
