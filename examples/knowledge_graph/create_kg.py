import os, sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from etk.etk import ETK
from etk.knowledge_graph import KnowledgeGraph, KgSchema
from etk.extractors.glossary_extractor import GlossaryExtractor
import json

sample_input = {
    "projects": [
        {
            "name": "etk",
            "description": "version 2 of etk, implemented by Runqi12 Shao, Dongyu Li, Sylvia lin, Amandeep and others."
        },
        {
            "name": "rltk",
            "description": "record linkage toolkit, implemented by Pedro, Mayank, Yixiang and several students."
        }
    ]
}

etk = ETK()
doc = etk.create_document(sample_input)

descriptions = doc.select_segments("projects[*].description")
projects = doc.select_segments("projects[*]")

name_extractor = GlossaryExtractor(etk.load_glossary("./names.txt"), "name_extractor", etk.default_tokenizer, case_sensitive=False, ngrams=1)

for d, p in zip(descriptions, projects):
    names = doc.invoke_extractor(name_extractor, d)
    p.store_extractions(names, "members")

master_config = etk.load_master_config("master_config.json")

kg_schema = KgSchema(master_config)

knowledge_graph = KnowledgeGraph(kg_schema, doc, etk)
knowledge_graph.add_value("actor", "projects[*].members[*]")
print(json.dumps(knowledge_graph.get_kg(), indent=2))
