import os, sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from etk.etk import ETK
from etk.knowledge_graph import KGSchema
from etk.etk_module import ETKModule
from etk.ontology_api import Ontology, rdf_generation
from rdflib.namespace import RDF

class ExampleETKModule(ETKModule):
    def process_document(self, doc: Document):
        extractions = []
        doc.kg.add_triple(URI(doc.value.doc_id), URI('rdfs:label'), Literal(extraction][0]))
        t = Triples(URI(doc.value.doc_id))
        t.add_property(...)
        doc.kg.add_triples(triples)
        t = Triples(URI(doc.value.doc_id))
        t.add_property(...)
        doc.kg.add_triples(triples)

        b = BNode()
        t = Triples(b)
        t2 = Triples(URI(...))
        t2.add_property(URI(), b)

        doc.kg.serializer()

if __name__ == "__main__":
    kg_schema = KGSchema()
    kg_schema.add_schema('filepath.owl', format='xml')
    kg_schema.ontology.add_triples(triples)
    kg_schema.ontology.namespace_manager.bind('cco', 'http://example.com/')

    kg_schema.add_schema('master_config.json', format='master_config')

    etk = ETK(modules=ExampleETKModule, kg_schema=kg_schema)
    input_data = {'doc_id': '1', 'data': json.loads(sample_input)}
    doc = etk.create_document(input_data)
    docs = etk.process_ems(doc)

