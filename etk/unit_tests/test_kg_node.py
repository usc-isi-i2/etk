import unittest
from etk.knowledge_graph.node import URI, BNode, Literal, LiteralType


class TestKGNode(unittest.TestCase):
    def test_nodes(self):
        uri = URI('rdf:type')
        uri2 = URI('rdf:type')
        self.assertEqual(uri.value, 'rdf:type')
        self.assertEqual(len({uri, uri2}), 1)

        bnode = BNode()
        bnode2 = BNode()
        self.assertNotEqual(bnode, bnode2)
        self.assertEqual(len({bnode, bnode2}), 2)

        lit = Literal('name', 'en', 'xsd:string')
        lit2 = Literal('nombre', 'es')
        lit3 = Literal('name', 'en', 'xsd:string')
        self.assertNotEqual(lit, lit2)
        self.assertEqual(lit, lit3)
        self.assertEqual(len({lit, lit2, lit3}), 2)
        self.assertEqual(lit.type, LiteralType('string'))
