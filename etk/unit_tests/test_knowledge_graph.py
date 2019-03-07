import unittest
import json
from etk.knowledge_graph.schema import KGSchema
from etk.etk import ETK
from etk.etk_exceptions import KGValueError
from datetime import date, datetime
from etk.knowledge_graph.ontology import Ontology
from etk.knowledge_graph.namespacemanager import DIG
from etk.knowledge_graph.node import URI, BNode, Literal, LiteralType
from etk.knowledge_graph.subject import Subject
import rdflib


class TestKnowledgeGraph(unittest.TestCase):
    def setUp(self):
        sample_doc = {
            "projects": [
                {
                    "name": "etk",
                    "description": "version 2 of etk, implemented by Runqi12 Shao, Dongyu Li, Sylvia lin, Amandeep and others.",
                    "members": ["dongyu", "amandeep", "sylvia", "Runqi12"],
                    "date": "2007-12-05",
                    "place": "columbus:georgia:united states:-84.98771:32.46098",
                    "s": "segment_test_1"
                },
                {
                    "name": "rltk",
                    "description": "record linkage toolkit, implemented by Pedro, Mayank, Yixiang and several students.",
                    "members": ["mayank", "yixiang"],
                    "date": ["2007-12-05T23:19:00"],
                    "cost": -3213.32,
                    "s": "segment_test_2"
                }
            ]
        }
        kg_schema = KGSchema(json.load(open('etk/unit_tests/ground_truth/test_config.json')))

        etk = ETK(kg_schema)
        self.doc = etk.create_document(sample_doc, doc_id="http://isi.edu/default-ns/projects")

    def test_add_segment_kg(self) -> None:
        sample_doc = self.doc
        segments = sample_doc.select_segments("projects[*].s")
        for segment in segments:
            sample_doc.kg.add_value("segment", segment.value)
        expected_segments = ["segment_test_1", "segment_test_2"]
        self.assertIn(sample_doc.kg.value["segment"][0]["key"], expected_segments)
        self.assertIn(sample_doc.kg.value["segment"][1]["key"], expected_segments)
        # self.assertIn('provenances', sample_doc.value)
        # provenances = sample_doc.value['provenances']
        # self.assertEqual(len(provenances), 2)
        # self.assertEqual(provenances[0]['reference_type'], 'location')

    def test_KnowledgeGraph(self) -> None:
        sample_doc = self.doc

        try:
            for member in sample_doc.select_segments("projects[*].members[*]"):
                sample_doc.kg.add_value("developer", member.value)
        except KGValueError:
            pass

        # TODO: for property with range: xsd:date, auto convert to literal(type=date)
        # try:
        #     for date_ in sample_doc.select_segments("projects[*].date[*]"):
        #         sample_doc.kg.add_value("test_date", date_.value)
        # except KGValueError:
        #     pass

        # TODO: for date obj, auto convert to literal(type=date)
        # try:
        #     sample_doc.kg.add_value("test_add_value_date", date(2018, 3, 28))
        #     sample_doc.kg.add_value("test_add_value_date", {})
        #     sample_doc.kg.add_value("test_add_value_date", datetime(2018, 3, 28, 1, 1, 1))
        # except KGValueError:
        #     pass

        try:
            for place in sample_doc.select_segments("projects[*].place"):
                sample_doc.kg.add_value("test_location", place.value)
        except KGValueError:
            pass

        expected_developers = [
            {
                "key": "amandeep",
                "value": "amandeep"
            },
            {
                "key": "dongyu",
                "value": "dongyu"
            },
            {
                "key": "mayank",
                "value": "mayank"
            },
            {
                "key": "runqi12",
                "value": "Runqi12"
            },
            {
                "key": "sylvia",
                "value": "sylvia"
            },
            {
                "key": "yixiang",
                "value": "yixiang"
            }
        ]

        expected_date = [
            {
                "value": "2007-12-05T00:00:00",
                "key": "2007-12-05T00:00:00"
            },
            {
                "value": "2007-12-05T23:19:00",
                "key": "2007-12-05T23:19:00"
            }
        ]

        expected_add_value_date = [
            {
                "value": "2018-03-28",
                "key": "2018-03-28"
            },
            {
                "value": "2018-03-28T01:01:01",
                "key": "2018-03-28T01:01:01"
            }
        ]

        expected_location = [
            {
                "value": "columbus:georgia:united states:-84.98771:32.46098",
                "key": "columbus:georgia:united states:-84.98771:32.46098"
            }
        ]

        self.assertEqual(expected_developers,
                         sorted(sample_doc.kg.value["developer"], key=lambda x: x['key']))
        # self.assertEqual(expected_date, sample_doc.kg.value["test_date"])
        self.assertEqual(expected_location, sample_doc.kg.value["test_location"])
        # self.assertEqual(expected_add_value_date, sample_doc.kg.value["test_add_value_date"])

    def test_add_value_empty(self):
        pass
        # TODO: auto convert 0.0 to Literal(type=decimal) and convert back with kg.value
        # self.doc.kg.add_value('test_zero', 0.0)
        # self.assertEqual(self.doc.kg.value['test_zero'][0]['value'], 0.0)


class TestKnowledgeGraphWithOntology(unittest.TestCase):
    def setUp(self):
        ontology_content = '''
            @prefix : <http://dig.isi.edu/ontologies/dig/> .
            @prefix dig: <http://dig.isi.edu/ontologies/dig/> .
            @prefix owl: <http://www.w3.org/2002/07/owl#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
            @prefix schema: <http://schema.org/> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
            :Person a owl:Class ;
                rdfs:subClassOf :Actor, :Biological_Object ;
                :common_properties :label, :title, :religion ; .
            :has_name a owl:DatatypeProperty ;
                schema:domainIncludes :Person ;
                schema:rangeIncludes xsd:string ; .
            :has_child a owl:ObjectProperty ;
                schema:domainIncludes :Person ;
                schema:rangeIncludes :Person ; .
            '''
        kg_schema = KGSchema()
        kg_schema.add_schema(ontology_content, 'ttl')
        etk = ETK(kg_schema=kg_schema)
        self.doc = etk.create_document(dict(), doc_id='http://xxx/1', type_=[URI('dig:Person')])

    def test_valid_kg(self):
        kg = self.doc.kg
        self.assertIsInstance(kg._g, rdflib.Graph)
        self.assertEqual(len(kg._g), 1)
        self.assertIn((rdflib.URIRef('http://xxx/1'), rdflib.RDF.type, DIG.Person), kg._g)

    def test_valid_ontology(self):
        onto = self.doc.etk.kg_schema.ontology
        self.assertIsInstance(onto._g, rdflib.Graph)
        self.assertEqual(len(onto._g), 12)

    def test_add_types(self):
        kg = self.doc.kg
        self.assertIn((rdflib.URIRef('http://xxx/1'), rdflib.RDF.type, DIG.Person), kg._g)
        kg.add_types(URI('dig:Male'))
        self.assertIn((rdflib.URIRef('http://xxx/1'), rdflib.RDF.type, DIG.Male), kg._g)
        kg.add_types([URI('dig:Human')])
        self.assertIn((rdflib.URIRef('http://xxx/1'), rdflib.RDF.type, DIG.Human), kg._g)

    def test_add_subject(self):
        kg = self.doc.kg
        subject = Subject(URI('http://xxx/1'))
        subject.add_property(URI('dig:name'), Literal('Jack', lang='en'))
        subject.add_property(URI('dig:age'), Literal('18', type_=LiteralType.int))
        friend = Subject(BNode())
        friend.add_property(URI('rdf:type'), Literal('dig:Person'))
        friend.add_property(URI('dig:name'), Literal('José', lang='es'))
        friend.add_property(URI('dig:age'), Literal('19', type_=LiteralType.int))
        friend.add_property(URI('dig:friend'), subject)
        subject.add_property(URI('dig:friend'), friend)
        kg.add_subject(subject)
        self.assertEqual(len(kg._g), 8)
        self.assertIn((rdflib.URIRef('http://xxx/1'), DIG.name, rdflib.Literal('Jack', lang='en')), kg._g)
