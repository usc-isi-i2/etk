import unittest
from rdflib import Graph, URIRef
from etk.ontology_namespacemanager import OntologyNamespaceManager
from etk.ontology_namespacemanager import PrefixNotFoundException, PrefixAlreadyUsedException


class TestOntologyNamespaceManager(unittest.TestCase):
    def test_ontology_namespace_parse_uri(self):
        nm = OntologyNamespaceManager(Graph())
        uri = URIRef('http://dig.isi.edu/ontologies/dig/Event')
        self.assertEqual(uri, nm.parse_uri(uri))
        for case in ('http://dig.isi.edu/ontologies/dig/Event', 'dig:Event'):
            self.assertEqual(uri, nm.parse_uri(case))
        with self.assertRaises(PrefixNotFoundException):
            nm.parse_uri(':Event')
        nm.bind('', 'http://dig.isi.edu/ontologies/dig/')
        self.assertEqual(uri, nm.parse_uri(':Event'))
        self.assertEqual(uri, nm.parse_uri('Event'))

    def test_ontology_namespace_bind(self):
        correct_content = '@prefix : <http://w.org/> . @prefix schema: <http://schema.org/> .'
        wrong_content = '@prefix schema: <http://dig.schema.org/> .'
        nm = OntologyNamespaceManager(Graph())
        namespace = {x[0] for x in nm.namespaces()}
        self.assertNotIn('', namespace)
        for name in ('owl', 'rdfs', 'rdf', 'schema', 'xsd', 'xml', 'dig'):
            self.assertIn(name, namespace)
        with self.assertRaises(PrefixAlreadyUsedException):
            nm.graph.parse(data=wrong_content, format='ttl')
        nm.graph.parse(data=correct_content, format='ttl')
        namespace = {x[0] for x in nm.namespaces()}
        self.assertIn('', namespace)
