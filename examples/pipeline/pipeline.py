import os, sys, json
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from etk.etk import ETK
from etk.knowledge_graph import KgSchema


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
    ],
    "date_description": {"text": "el 29 de febrero de 1996 vs lunes, el 24 de junio, 2013 vs 3 de octubre de 2017"}
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
        },
        "date": {
            "type": "date"
        }
    }
}

kg_schema = KgSchema(master_config)

etk = ETK(kg_schema, "./extraction_modules/")

doc = etk.create_document(sample_input)

doc, knowledge_graph = etk.process_ems(doc)

print(json.dumps(doc.value, indent=2))