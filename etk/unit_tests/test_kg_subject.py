import unittest
from etk.knowledge_graph.node import URI, BNode, Literal
from etk.knowledge_graph.subject import Subject, Reification, InvalidParameter


class TestKGTriples(unittest.TestCase):
    def test_subject(self):
        s = URI('ex:ex1')
        t = Subject(s)
        lit = Literal('jack', 'en', 'xsd:string')
        t.add_property(URI('rdf:type'), URI('dig:Person'))
        t.add_property(URI('dig:name'), lit)
        self.assertEqual(t.subject, s)
        self.assertEqual(len(t._resource), 2)
        self.assertIn(URI('rdf:type'), t._resource)
        self.assertIn(URI('dig:name'), t._resource)

        # remove property
        self.assertFalse(t.remove_property(URI('rdf:type'), URI('dig:Male')))
        self.assertTrue(t.remove_property(URI('rdf:type'), URI('dig:Person')))
        self.assertNotIn(URI('rdf:type'), t._resource)

        self.assertTrue(t.remove_property(URI('dig:name')))
        self.assertNotIn(URI('dig:name'), t._resource)

        self.assertFalse(t.remove_property(URI('rdf:type')))
        self.assertFalse(t.remove_property(URI('dig:name')))

    def test_subject_exception(self):
        with self.assertRaises(InvalidParameter):
            Subject(None)

        with self.assertRaises(InvalidParameter):
            Subject('ex:ex1')

        with self.assertRaises(InvalidParameter):
            Subject(Literal('test'))

        s = Subject(URI('ex:ex1'))
        with self.assertRaises(InvalidParameter):
            s.add_property(URI('rdf:type'), 'dig:Person')

        with self.assertRaises(InvalidParameter):
            s.add_property(BNode(), URI('dig:Person'))

    def test_subject_reification(self):
        s = URI('ex:ex1')
        t = Subject(s)
        lit = Literal('jack', 'en', 'xsd:string')
        statement1 = t.add_property(URI('rdf:type'), URI('dig:Person'))
        statement2 = t.add_property(URI('dig:name'), lit, Reification(URI('digg:name')))
        statement3 = t.add_property(URI('dig:name'), lit, Reification(URI('diga:name'), URI('digb:name')))
        statement4 = t.add_property(URI('dig:name'), lit, Reification(URI('digg:name'), statement=URI('ex:assert1')))
        self.assertEqual(t.subject, s)
        self.assertEqual(len(t._resource), 4)
        self.assertIn(URI('rdf:type'), t._resource)
        self.assertIn(URI('dig:name'), t._resource)
        self.assertIn(URI('digg:name'), t._resource)
        self.assertIn(URI('diga:name'), t._resource)
        self.assertNotIn(URI('digb:name'), t._resource)
        self.assertNotIsInstance(statement1, Subject)
        self.assertIsInstance(statement2, Subject)
        self.assertIn(URI('digg:name'), statement2._resource)
        self.assertIsInstance(statement3, Subject)
        self.assertIsInstance(statement3.subject, BNode)
        self.assertIsInstance(statement4, Subject)
        self.assertIsInstance(statement4.subject, URI)

        # exception
        with self.assertRaises(InvalidParameter):
            Reification(None)
        with self.assertRaises(InvalidParameter):
            Reification(URI('dig:name'), 3)
        with self.assertRaises(InvalidParameter):
            Reification(URI('dig:name'), statement='')

    def test_subject_reification_back_compatible(self):
        s = URI('ex:ex1')
        t = Subject(s)
        lit = Literal('jack', 'en', 'xsd:string')
        statement1 = t.add_property(URI('rdf:type'), URI('dig:Person'))
        statement2 = t.add_property(URI('dig:name'), lit, reify=URI('digg:name'))
        statement3 = t.add_property(URI('dig:name'), lit, reify=(URI('digg:name'), None))
        statement4 = t.add_property(URI('dig:name'), lit, reify=(URI('digg:name'), URI('ex:assert1')))
        self.assertEqual(t.subject, s)
        self.assertEqual(len(t._resource), 3)
        self.assertIn(URI('rdf:type'), t._resource)
        self.assertIn(URI('dig:name'), t._resource)
        self.assertIn(URI('digg:name'), t._resource)
        self.assertNotIsInstance(statement1, Subject)
        self.assertIsInstance(statement2, Subject)
        self.assertIn(URI('digg:name'), statement2._resource)
        self.assertIsInstance(statement3, Subject)
        self.assertIsInstance(statement3.subject, BNode)
        self.assertIsInstance(statement4, Subject)
        self.assertIsInstance(statement4.subject, URI)

        # exception
        with self.assertRaises(InvalidParameter):
            t.add_property(URI('dig:name'), lit, reify=(None, URI('s')))
        with self.assertRaises(InvalidParameter):
            t.add_property(URI('dig:name'), lit, reify=(URI('digg:name'), 3))
        with self.assertRaises(InvalidParameter):
            t.add_property(URI('dig:name'), lit, reify=(URI('digg:name'), ''))

