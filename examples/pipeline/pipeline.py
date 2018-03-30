import os, sys, json
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from etk.etk import ETK


etk=ETK()
em_lst = etk.load_ems("./extraction_modules/")

sample_input = {
    "projects": [
        {
            "name": "etk",
            "description": "version 2 of etk, implemented by Runqi, Dongyu, Sylvia, Amandeep and others."
        },
        {
            "name": "rltk",
            "description": "record linkage toolkit, implemented by Pedro, Mayank, Yixiang and several students."
        }
    ]
}

doc = etk.create_document(sample_input)
for a_em in em_lst:
    if a_em.document_selector(doc):
        a_em.process_document(doc)

print(json.dumps(doc.value, indent=2))