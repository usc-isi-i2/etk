import os, sys, json
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from etk.etk import ETK
from etk.knowledge_graph import KGSchema


sample_input = {
    "doc_id": "DASJDASH",
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
        },
        "id": {
            "type": "kg_id"
        }
    }
}

kg_schema = KGSchema(master_config)

etk = ETK(kg_schema, ["./extraction_modules/"])

doc = etk.create_document(sample_input)

docs = etk.process_ems(doc)

print(json.dumps(docs[0].value, indent=2))