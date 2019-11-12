import unittest
from etk.wikidata.entity import *
from etk.knowledge_graph import URI


class TestMultiNamespaceEntity(unittest.TestCase):
    def test_entity(self):
        i = WDItem('Q34', namespace='dm')
        self.assertEqual(i.value, URI('dm:Q34'))
        self.assertEqual(i.type, URI('wikibase:WikibaseItem'))
        self.assertIsNone(i.full_value)
        self.assertIsNone(i.normalized_value)

        p = WDProperty('P69', property_type=Datatype.StringValue, namespace='dm')
        self.assertEqual(p.value, URI('dm:P69'))
        self.assertEqual(p.type, URI('wikibase:WikibaseProperty'))
        self.assertIsNone(p.full_value)
        self.assertIsNone(p.normalized_value)
