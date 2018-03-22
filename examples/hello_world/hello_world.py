import json, os, sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from etk.etk import ETK
from etk.extractors.glossary_extractor import GlossaryExtractor
from etk.extractors.date_extractor import DateExtractor

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
    ],
    "members": [
        {
            "name": "Dongyu Li",
            "description": "03/05/2018: I went to USC on Aug 20th, 2016 and will graduate on 2018, May 11. My birthday is 29-04-1994."
        }
    ]
}

etk = ETK()
doc = etk.create_document(sample_input)

# example for glossary extractor:
name_extractor = GlossaryExtractor(etk.load_glossary("./names.txt"), "name_extractor", etk.default_tokenizer, case_sensitive=False, ngrams=1)

descriptions = doc.select_segments("projects[*].description")
projects = doc.select_segments("projects[*]")

for d, p in zip(descriptions, projects):
    names = doc.invoke_extractor(name_extractor, d)
    p.store_extractions(names, "members")

# example for date extractor:
date_extractor = DateExtractor('test_date_parser')
member_descriptions = doc.select_segments("members[*].description")
members = doc.select_segments("members[*]")

for m_d, m in zip(member_descriptions, members):
    dates = doc.invoke_extractor(date_extractor, m_d, ignore_future_dates=False, ignore_past_years=40)
    m.store_extractions(dates, "related_dates")


print(json.dumps(sample_input, indent=2))
