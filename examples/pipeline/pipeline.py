import os, sys, json
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from etk.etk import ETK
from etk.knowledge_graph import KgSchema, KnowledgeGraph

etk=ETK()
em_lst = etk.load_ems("./extraction_modules/")

sample_input = {
    "projects": [
        {
            "name": "etk",
            "description": "version 2 of etk, implemented by Runqi Shao, Dongyu Li, Sylvia Ling, Amandeep Singh and others."
        },
        {
            "name": "rltk",
            "description": "record linkage toolkit, implemented by Pedro Szekely, Mayank K., Yixiang Y. and several students."
        }
    ]
}

master_config = {
            "fields": {
                "developer": {
                    "type": "string"
                },
                "student_developer": {
                    "type": "string"
                },
                "spacy_name": {
                    "type": "string"
                }
            }
        }

doc = etk.create_document(sample_input)

kg_schema = KgSchema(master_config)
knowledge_graph = KnowledgeGraph(kg_schema, doc, etk)

for a_em in em_lst:
    if a_em.document_selector(doc):
        a_em.process_document(doc, knowledge_graph)

print(json.dumps(knowledge_graph.get_kg(), indent=2))