import unittest
from rdflib import Graph, URIRef
import rdflib.namespace as rlns
from etk.knowledge_graph.namespacemanager import NamespaceManager, SCHEMA, DIG
from etk.knowledge_graph.namespacemanager import PrefixNotFoundException, PrefixAlreadyUsedException


class TestKGNamespaceManager(unittest.TestCase):
    def test_namespace_parse_uri(self):
        nm = NamespaceManager(Graph())
        nm.bind('dig', DIG)
        uri = URIRef('http://dig.isi.edu/ontologies/dig/Event')
        self.assertEqual(uri, nm.parse_uri(uri))
        for case in ('http://dig.isi.edu/ontologies/dig/Event', 'dig:Event'):
            self.assertEqual(uri, nm.parse_uri(case))
        with self.assertRaises(PrefixNotFoundException):
            nm.parse_uri(':Event')
        nm.bind('', 'http://dig.isi.edu/ontologies/dig/')
        self.assertEqual(uri, nm.parse_uri(':Event'))
        self.assertEqual(uri, nm.parse_uri('Event'))

    def test_namespace_bind(self):
        correct_content = '@prefix : <http://w.org/> . @prefix schema: <http://schema.org/> .'
        wrong_content = '@prefix schema: <http://dig.schema.org/> .'
        nm = NamespaceManager(Graph())
        nm.bind('owl', rlns.OWL)
        nm.bind('schema', SCHEMA)
        nm.bind('dig', DIG)
        namespace = {x[0] for x in nm.namespaces()}
        self.assertNotIn('', namespace)
        for name in ('owl', 'rdfs', 'rdf', 'schema', 'xsd', 'xml', 'dig'):
            self.assertIn(name, namespace)
        with self.assertRaises(PrefixAlreadyUsedException):
            nm.graph.parse(data=wrong_content, format='ttl')
        nm.graph.parse(data=correct_content, format='ttl')
        namespace = {x[0] for x in nm.namespaces()}
        self.assertIn('', namespace)

    def test_namespace_split_uri(self):
        nm = NamespaceManager(Graph())
        nm.bind('rdf', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#')
        prefix, property_ = nm.split_uri('http://www.w3.org/1999/02/22-rdf-syntax-ns#label')
        self.assertEqual(prefix, 'rdf')
        self.assertEqual(property_, 'label')
