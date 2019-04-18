import unittest
from etk.knowledge_graph.graph import Graph
from etk.knowledge_graph.subject import Subject
from etk.knowledge_graph.node import URI, BNode, Literal, LiteralType


class TestKGGraph(unittest.TestCase):
    def test_graph(self):
        g = Graph()
        g.bind('ex', 'http://ex.com/')
        g.bind('rdf', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#')
        g.bind('foaf', 'http://xmlns.com/foaf/0.1/')

        t_nest = Subject(BNode())
        t_nest.add_property(URI('rdf:type'), URI('foaf:Person'))
        t_nest.add_property(URI('foaf:name'), Literal('Alfred'))
        t_nest.add_property(URI('foaf:age'), Literal('36', type_=LiteralType.int))

        t = Subject(URI('ex:john'))
        t.add_property(URI('rdf:type'), URI('foaf:Person'))
        t.add_property(URI('foaf:name'), Literal('John'))
        t.add_property(URI('foaf:age'), Literal('12', type_=LiteralType.int))
        t.add_property(URI('foaf:knows'), t_nest)
        g.add_subject(t)

        self.assertEqual(len(g._g), 7)
        # print(g.serialize())
        # print('========================================================================')
        # print(g.serialize('nt'))
        # print('========================================================================')
        # context = {'@vocab': 'http://xmlns.com/foaf/0.1/'}
        # print(g.serialize('json-ld', context=context, auto_compact=True))
