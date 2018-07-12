import os, sys, json
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from etk.etk import ETK
from etk.knowledge_graph import KGSchema
from examples.config_to_em.em_base_generator import EmBaseGenerator


ebg = EmBaseGenerator('template.tpl')
ebg.generate_em_base('master_config.json', 'ems/em_base.py')

kg_schema = KGSchema(json.load(open("master_config.json", "r")))

etk = ETK(kg_schema, ["./ems"])

doc = etk.create_document(json.load(open('sample_html.jl', 'r')))

docs = etk.process_ems(doc)

print(json.dumps(docs[0].value, indent=2))