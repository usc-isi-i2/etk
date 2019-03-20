import unittest
from rdflib import Graph, URIRef
import rdflib.namespace as rlns
from etk.knowledge_graph.namespacemanager import NamespaceManager, SCHEMA, DIG
from etk.knowledge_graph.namespacemanager import WrongFormatURIException, PrefixNotFoundException
from etk.knowledge_graph.node import URI


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
        self.assertEqual(uri, nm.parse_uri(URI(':Event')))
        with self.assertRaises(WrongFormatURIException):
            nm.parse_uri(None)

    def test_namespace_bind(self):
        correct_content = '@prefix : <http://w.org/> . @prefix schema: <http://schema.org/> .'
        replace_content = '@prefix schema: <http://dig.schema.org/> .'
        nm = NamespaceManager(Graph())
        nm.bind('owl', rlns.OWL)
        nm.bind('schema', SCHEMA)
        nm.bind('dig', DIG)
        namespace = {x[0] for x in nm.namespaces()}
        self.assertNotIn('', namespace)
        for name in ('owl', 'rdfs', 'rdf', 'schema', 'xsd', 'xml', 'dig'):
            self.assertIn(name, namespace)

        nm.graph.parse(data=correct_content, format='ttl')
        namespace = {x[0] for x in nm.namespaces()}
        self.assertIn('', namespace)

        nm.graph.parse(data=replace_content, format='ttl')
        namespace = {x[0] for x in nm.namespaces()}
        self.assertIn('schema', namespace)
        self.assertNotEqual(nm.store.namespace('schema'), SCHEMA)
        self.assertEqual(nm.store.namespace('schema'), URIRef('http://dig.schema.org/'))

    def test_namespace_split_uri(self):
        nm = NamespaceManager(Graph())
        nm.bind('rdf', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#')
        prefix, property_ = nm.split_uri('http://www.w3.org/1999/02/22-rdf-syntax-ns#label')
        self.assertEqual(prefix, 'rdf')
        self.assertEqual(property_, 'label')

    def test_namespace_check_uriref(self):
        nm = NamespaceManager(Graph())
        uri = URIRef('http://dig.isi.edu/ontologies/dig/Event')
        self.assertEqual(nm.check_uriref(uri), uri)

        text = 'http://dig.isi.edu/ontologies/dig/Event'
        self.assertEqual(nm.check_uriref(text), uri)

        text = URI('http://dig.isi.edu/ontologies/dig/Event')
        self.assertEqual(nm.check_uriref(text), uri)
