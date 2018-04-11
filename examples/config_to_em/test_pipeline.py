import os, sys, json
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from etk.etk import ETK
from etk.knowledge_graph import KGSchema


kg_schema = KGSchema(json.load(open("master_config.json", "r")))

etk = ETK(kg_schema, "./ems")

doc = etk.create_document(json.load(open('sample_html.jl', 'r')))

doc, knowledge_graph = etk.process_ems(doc)

print(json.dumps(doc.value, indent=2))