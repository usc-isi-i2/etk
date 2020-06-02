import unittest
from etk.wikidata.value import *
from etk.knowledge_graph import URI


class TestWikidataValue(unittest.TestCase):
    def test_entity_value(self):
        i = Item('Q34')
        self.assertEqual(i.value, URI('wd:Q34'))
        self.assertEqual(i.type, URI('wikibase:WikibaseItem'))
        self.assertIsNone(i.full_value)
        self.assertIsNone(i.normalized_value)

        p = Property('P69')
        self.assertEqual(p.value, URI('wd:P69'))
        self.assertEqual(p.type, URI('wikibase:WikibaseProperty'))
        self.assertIsNone(p.full_value)
        self.assertIsNone(p.normalized_value)

    def test_time_value(self):
        tv = TimeValue('2019-05-05T00:00:00', Item('Q1985727'), Precision.day, 0)
        self.assertEqual(tv.value, Literal('2019-05-05T00:00:00', type_=LiteralType.dateTime))
        self.assertIsInstance(tv.full_value, Subject)
        self.assertIsNone(tv.normalized_value)
        self.assertEqual(tv.type, URI('wikibase:Time'))

    def test_external_identifier(self):
        ei = ExternalIdentifier('/m/0282x', URLValue('http://g.co/kg/m/0282x'))
        self.assertEqual(ei.value, Literal('/m/0282x', type_=LiteralType.string))
        self.assertIsNone(ei.full_value)
        self.assertIsInstance(ei.normalized_value, URI)
        self.assertIsInstance(ei.normalized_value, URLValue)
        self.assertEqual(ei.type, URI('wikibase:ExternalId'))

    def test_quantity_value(self):
        qv = QuantityValue(1.5)
        print(qv._v_name())
        self.assertEqual(qv.value, Literal('1.5', type_=LiteralType.decimal))
        self.assertIsInstance(qv.full_value, Subject)
        self.assertIsInstance(qv.normalized_value, Subject)
        self.assertEqual(qv.normalized_value, qv.full_value)
        self.assertEqual(qv.type, URI('wikibase:Quantity'))

    def test_quantity_value_2(self):
        qv = QuantityValue(1.5, Item('Q828224')) # kilometer
        self.assertEqual(qv.value, Literal('1.5', type_=LiteralType.decimal))
        self.assertIsInstance(qv.full_value, Subject)
        self.assertIsInstance(qv.normalized_value, Subject)
        self.assertEqual(qv.normalized_value, qv.full_value)
        self.assertEqual(qv.type, URI('wikibase:Quantity'))
        self.assertEqual(qv.unit.value.value[3:], 'Q828224')

    def test_string_value(self):
        sv = StringValue('blabla')
        self.assertEqual(sv.value, Literal('blabla', type_=LiteralType.string))
        self.assertIsNone(sv.normalized_value)
        self.assertIsNone(sv.full_value)
        self.assertEqual(sv.type, URI('wikibase:String'))

    def test_url_value(self):
        uv = URLValue('http://www.isi.edu/')
        self.assertIsInstance(uv, URI)
        self.assertEqual(uv.value, URI('http://www.isi.edu/'))
        self.assertIsNone(uv.full_value)
        self.assertIsNone(uv.normalized_value)
        self.assertEqual(uv.type, URI('wikibase:Url'))

    def test_globe_coordinate(self):
        gc = GlobeCoordinate(45.0, -1, 1, Item('Q405'))
        self.assertIsInstance(gc.full_value, Subject)
        self.assertIsNone(gc.normalized_value)
        self.assertEqual(gc.type, URI('wikibase:GlobeCoordinate'))
        self.assertEqual(gc.value.value, '<http://www.wikidata.org/entity/Q405> Point(45.0 -1)')

    def test_globe_coordinate_2(self):
        gc = GlobeCoordinate(45.0, -1, 1)
        self.assertIsInstance(gc.full_value, Subject)
        self.assertIsNone(gc.normalized_value)
        self.assertEqual(gc.type, URI('wikibase:GlobeCoordinate'))
        self.assertEqual(gc.value.value, 'Point(45.0 -1)')

    def test_monolingual_text(self):
        mt = MonolingualText('Gracias', lang='es')
        self.assertEqual(mt.value, Literal('Gracias', lang='es'))
        self.assertIsNone(mt.full_value)
        self.assertIsNone(mt.normalized_value)
        self.assertEqual(mt.type, URI('wikibase:Monolingualtext'))
