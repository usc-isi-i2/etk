import json, os, sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from etk.etk import ETK
from etk.extractors.glossary_extractor import GlossaryExtractor

sample_input = {
    "projects": [
        {
            "name": "etk",
            "description": "version 2 of etk, implemented by Runqi, Dongyu, Sylvia, Amandeep, Anika and others."
        },
        {
            "name": "rltk",
            "description": "record linkage toolkit, implemented by Pedro, Mayank, Yixiang and several students."
        }
    ]
}

etk = ETK()
doc = etk.create_document(sample_input)

# example for glossary extractor:
name_extractor = GlossaryExtractor(etk.load_glossary("./names.txt"), "name_extractor", etk.default_tokenizer, case_sensitive=False, ngrams=1)
student_extractor = GlossaryExtractor(etk.load_glossary("./student.txt"), "student_extractor", etk.default_tokenizer, case_sensitive=False, ngrams=1)

descriptions = doc.select_segments("projects[*].description")
projects = doc.select_segments("projects[*]")

for d, p in zip(descriptions, projects):

    # First phase of extraction
    names = doc.invoke_extractor(name_extractor, d)
    print([name_extraction.value for name_extraction in names])
    p.store_extractions(names, "members")

    # Second phase of extraction
    students = []
    for name_extraction in names:
        students += doc.invoke_extractor(student_extractor, name_extraction)
    p.store_extractions(students, "students")


    print(json.dumps(sample_input, indent=2))
