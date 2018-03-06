import os, sys, json, codecs
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from etk.etk import ETK
from etk.extractors.table_extractor import TableExtractor

sample_html = json.load(codecs.open('./table_doc.jl', 'r')) # read sample file from disk

etk = ETK()
doc = etk.create_document(sample_html, mime_type="text/html", url="http://ex.com/123")

table_extractor = TableExtractor()

d = doc.select_segments("$.raw_content")[0]
root = doc.select_segments("$")[0]

tables = doc.invoke_extractor(table_extractor, d)
root.store_extractions(tables, "tables")

print(json.dumps(doc.cdr_document, indent=2))
