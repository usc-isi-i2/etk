import unittest
from etk.wikidata.statement import *
from etk.wikidata.value import *


class TestMultiNamespaceStatement(unittest.TestCase):
    def test_statement(self):
        s = Statement('Q34', Rank.Normal, namespace='dm')
        s.add_value('P31', Item('Q6256'), namespace='dm')
        s.add_value('P31', Item('Q6256'))
        s.add_qualifier('P1476', StringValue("Book"), namespace='dm')
        s.add_qualifier('P1476', StringValue("Book"))

        self.assertIn(URI('rdf:type'), s._resource)
        self.assertIn(URI('dmps:P31'), s._resource)
        self.assertIn(URI('wd:Q6256'), s._resource[URI('dmps:P31')])
        self.assertIn(URI('ps:P31'), s._resource)
        self.assertIn(URI('wd:Q6256'), s._resource[URI('ps:P31')])
        self.assertIn(URI('dmpq:P1476'), s._resource)
        self.assertIn(Literal("Book", type_=LiteralType.string), s._resource[URI('dmpq:P1476')])
        self.assertIn(URI('pq:P1476'), s._resource)
        self.assertIn(Literal("Book", type_=LiteralType.string), s._resource[URI('pq:P1476')])
        self.assertIsInstance(s, Subject)

    def test_reference(self):
        r = WDReference(namespace='dm')
        r.add_value('P170', URLValue('http://schema.org/'))
        r.add_value('P170', URLValue('http://schema.org/'), namespace='dm')

        self.assertIn(URI('pr:P170'), r._resource)
        self.assertIn(URI('dmpr:P170'), r._resource)

