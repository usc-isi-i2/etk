import unittest
from etk.knowledge_graph.graph import Graph
from etk.knowledge_graph.schema import KGSchema
from etk.knowledge_graph.subject import Subject
from etk.knowledge_graph.node import URI, BNode, Literal, LiteralType


MASTER_CONFIG_EXAMPLE = '''
{
    "fields": {
        "cameo_code": {
            "type": "string", 
            "description": "CAMEO code to describe the type of event in the ICEWS dataset"
        }, 
        "answer_option": {
            "type": "kg_id" 
        }, 
        "reference": {
            "type": "string"
        }
    }
}
'''

Ontology_EXAMPLE = '''
@prefix : <http://dig.isi.edu/ontologies/dig/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
:Entity a owl:Class ;
    rdfs:label "Entity" ;
    skos:definition """Everything that can be described, similar to owl:Thing.""" ;
    :common_properties :label, :title, :description ; .
'''


class TestKGSchema(unittest.TestCase):
    def test_kg_schema(self):
        schema = KGSchema()
        schema.add_schema(Ontology_EXAMPLE, 'ttl')

    def test_kg_schema_master_config(self):
        schema = KGSchema()
        schema.add_schema(MASTER_CONFIG_EXAMPLE, 'master_config')
        self.assertEqual(len(schema.fields), 3)
