import os, sys, json, codecs
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from etk.etk import ETK
from etk.extractors.table_extractor import TableExtractor, EntityTableDataExtraction

sample_html = json.load(codecs.open('./table_data.json', 'r')) # read sample file from disk

etk = ETK()
doc = etk.create_document(sample_html, mime_type="text/html", url="http://ex.com/123")

my_table_extractor = TableExtractor()

d = doc.select_segments("$.raw_content")[0]
root = doc.select_segments("$")[0]

tables = doc.invoke_extractor(my_table_extractor, d)
for t in tables:
    root.store_extractions([t], t.tag)

table_data_extractor = EntityTableDataExtraction()
table_data_extractor.add_glossary(etk.load_glossary("./resources/address_dict.txt"), "address")
table_data_extractor.add_glossary(etk.load_glossary("./resources/calibre_dict.txt"), "caliber")
table_data_extractor.add_glossary(etk.load_glossary("./resources/capacity_dict.txt"), "capacity")
table_data_extractor.add_glossary(etk.load_glossary("./resources/manufacturer_dict.txt"), "manufacturer")
table_data_extractor.add_glossary(etk.load_glossary("./resources/price_dict.txt"), "price")

tables = doc.select_segments("$.tables[*]")
root = doc.select_segments("$")[0]

for t in tables:
    extractions = doc.invoke_extractor(table_data_extractor, t)
    for e in extractions:
        root.store_extractions([e], e.tag)

print(json.dumps(doc.cdr_document, indent=2))
