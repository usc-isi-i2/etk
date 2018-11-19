import unittest
from etk.knowledge_graph.node import Node, URI, BNode, Literal, LiteralType
from etk.etk_exceptions import InvalidGraphNodeValueError, UnknownLiteralType


class TestKGNode(unittest.TestCase):
    def test_nodes_node(self):
        with self.assertRaises(InvalidGraphNodeValueError):
            Node(None)
        # equal
        node1 = Node('1')
        node2 = Node('2')
        self.assertEqual(node1, Node('1'))
        self.assertNotEqual(node1, node2)
        self.assertEqual(hash(node1), hash(Node('1')))
        self.assertNotEqual(hash(node1), hash(node2))
        # value
        self.assertEqual(node1.value, '1')
        # is_valid
        with self.assertRaises(NotImplementedError):
            node1.is_valid()

    def test_nodes_uri(self):
        uri = URI('rdf:type')
        uri2 = URI('rdf:type')
        with self.assertRaises(InvalidGraphNodeValueError):
            URI(None)
        # equal
        self.assertEqual(uri, uri2)
        # hash
        self.assertEqual(len({uri, uri2}), 1)
        # value
        self.assertEqual(uri.value, 'rdf:type')
        # is_valid
        self.assertTrue(uri.is_valid())

    def test_nodes_bnode(self):
        bnode = BNode()
        bnode2 = BNode()
        self.assertNotEqual(bnode, bnode2)
        self.assertEqual(len({bnode, bnode2}), 2)
        self.assertTrue(bnode.is_valid())

        # named bnode
        test_bnode = BNode('test')
        test_bnode2 = BNode('test')
        self.assertEqual(test_bnode, test_bnode2)
        self.assertNotEqual(test_bnode, bnode)
        self.assertEqual(hash(test_bnode), hash(test_bnode2))
        self.assertNotEqual(hash(test_bnode), hash(bnode))
        self.assertTrue(test_bnode.is_valid())

    def test_nodes_literal_type(self):
        xsd_int = LiteralType('xsd:int')
        self.assertEqual(xsd_int, LiteralType.int)
        self.assertEqual(xsd_int.value, LiteralType.int.value)
        complete_xsd_int = LiteralType('http://www.w3.org/2001/XMLSchema#int')
        self.assertEqual(complete_xsd_int, xsd_int)

        rdf_html = LiteralType('rdf:HTML')
        self.assertEqual(rdf_html, LiteralType.HTML)

        for s in (None, '', 'xxx'):
            with self.assertRaises(UnknownLiteralType):
                LiteralType(s)

        with self.assertRaises(UnknownLiteralType):
            LiteralType.xxx

    def test_nodes_literal(self):
        lit = Literal('name', 'en', LiteralType.string)
        lit2 = Literal('nombre', 'es')
        lit3 = Literal('name', 'en', 'xsd:string')
        self.assertNotEqual(lit, lit2)
        self.assertEqual(lit, lit3)
        self.assertEqual(len({lit, lit2, lit3}), 2)
        self.assertEqual(lit.type, LiteralType('xsd:string'))
        self.assertEqual(lit.raw_type, LiteralType.string.value)
        self.assertTrue(lit.is_valid())

    def test_nodes_init_with_nodes(self):
        uri = URI('rdf:type')
        self.assertIsInstance(URI(uri), URI)
        self.assertEqual(URI(uri), uri)

        bnode = BNode()
        self.assertIsInstance(BNode(bnode), BNode)
        self.assertEqual(BNode(bnode), bnode)

        lit = Literal('name', 'en', LiteralType.string)
        self.assertIsInstance(Literal(lit), Literal)
        self.assertEqual(Literal(lit), lit)
