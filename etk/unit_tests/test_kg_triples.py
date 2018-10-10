import unittest
from etk.knowledge_graph.node import URI, BNode, Literal
from etk.knowledge_graph.triples import Triples


class TestKGTriples(unittest.TestCase):
    def test_triple(self):
        s = URI('ex:ex1')
        t = Triples(s)
        lit = Literal('jack', 'en', 'xsd:string')
        t.add_property(URI('rdf:type'), URI('dig:Person'))
        t.add_property(URI('dig:name'), lit)

