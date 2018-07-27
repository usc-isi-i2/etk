import unittest, json
from etk.knowledge_graph import KGSchema
from etk.etk import ETK
from etk.etk_exceptions import KgValueError
from datetime import date, datetime
from etk.ontology_api import Ontology
from etk.ontology_namespacemanager import DIG


class TestKnowledgeGraph(unittest.TestCase):
    def setUp(self):
        sample_doc = {
            "projects": [
                {
                    "name": "etk",
                    "description": "version 2 of etk, implemented by Runqi12 Shao, Dongyu Li, Sylvia lin, Amandeep and others.",
                    "members": [
                        "dongyu",
                        "amandeep",
                        "sylvia",
                        "Runqi12"
                    ],
                    "date": "2007-12-05",
                    "place": "columbus:georgia:united states:-84.98771:32.46098",
                    "s": "segment_test_1"
                },
                {
                    "name": "rltk",
                    "description": "record linkage toolkit, implemented by Pedro, Mayank, Yixiang and several students.",
                    "members": [
                        "mayank",
                        "yixiang"
                    ],
                    "date": ["2007-12-05T23:19:00"],
                    "cost": -3213.32,
                    "s": "segment_test_2"
                }
            ]
        }
        kg_schema = KGSchema(json.load(open('etk/unit_tests/ground_truth/test_config.json')))

        etk = ETK(kg_schema)
        self.doc = etk.create_document(sample_doc)

    def test_add_segment_kg(self) -> None:
        sample_doc = self.doc
        segments = sample_doc.select_segments("projects[*].s")
        sample_doc.kg.add_value("segment", segments)
        expected_segments = ["segment_test_1", "segment_test_2"]
        self.assertTrue(sample_doc.kg.value["segment"][0]["key"] in expected_segments)
        self.assertTrue(sample_doc.kg.value["segment"][1]["key"] in expected_segments)
        self.assertTrue('provenances' in sample_doc.value)
        provenances = sample_doc.value['provenances']
        self.assertTrue(len(provenances) == 2)
        self.assertTrue(provenances[0]['reference_type'] == 'location')

    def test_KnowledgeGraph(self) -> None:
        sample_doc = self.doc

        try:
            sample_doc.kg.add_value("developer", json_path="projects[*].members[*]")
        except KgValueError:
            pass

        try:
            sample_doc.kg.add_value("test_date", json_path="projects[*].date[*]")
        except KgValueError:
            pass

        try:
            sample_doc.kg.add_value("test_add_value_date",
                                    value=[date(2018, 3, 28), {}, datetime(2018, 3, 28, 1, 1, 1)])
        except KgValueError:
            pass

        try:
            sample_doc.kg.add_value("test_location", json_path="projects[*].place")
        except KgValueError:
            pass

        try:
            sample_doc.kg.add_value("test_non_empty", value="")
            sample_doc.kg.add_value("test_non_empty", value="non-empty")
            sample_doc.kg.add_value("test_empty", value="", discard_empty=False)
            sample_doc.kg.add_value("test_empty", value="empty", discard_empty=False)
        except KgValueError:
            pass

        expected_developers = [
            {
                "value": "dongyu",
                "key": "dongyu"
            },
            {
                "value": "amandeep",
                "key": "amandeep"
            },
            {
                "value": "sylvia",
                "key": "sylvia"
            },
            {
                "value": "Runqi12",
                "key": "runqi12"
            },
            {
                "value": "mayank",
                "key": "mayank"
            },
            {
                "value": "yixiang",
                "key": "yixiang"
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

        expected_non_empty = [{"key": "non-empty", "value": "non-empty"}]
        expected_empty = [{"key": "", "value": ""}, {"key": "empty", "value": "empty"}]

        self.assertEqual(expected_developers, sample_doc.kg.value["developer"])
        self.assertEqual(expected_date, sample_doc.kg.value["test_date"])
        self.assertEqual(expected_location, sample_doc.kg.value["test_location"])
        self.assertEqual(expected_add_value_date, sample_doc.kg.value["test_add_value_date"])
        self.assertEqual(expected_non_empty, sample_doc.kg.value["test_non_empty"])
        self.assertEqual(expected_empty, sample_doc.kg.value["test_empty"])


class TestKnowledgeGraphWithOntology(unittest.TestCase):
    def setUp(self):
        ontology_content = '''
                @prefix : <http://dig.isi.edu/ontologies/dig/> .
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
        ontology = Ontology(ontology_content, validation=False, include_undefined_class=True, quiet=True)
        kg_schema = KGSchema(ontology.merge_with_master_config(dict()))
        etk = ETK(kg_schema=kg_schema, ontology=ontology, generate_json_ld=True)
        etk2 = ETK(kg_schema=kg_schema, ontology=ontology, generate_json_ld=False)
        self.doc = etk.create_document(dict(), doc_id='http://xxx/1', type_=[DIG.Person.toPython()])
        self.doc2 = etk2.create_document(dict(), doc_id='http://xxx/2', type_=[DIG.Person.toPython()])

    def test_valid_kg_jsonld(self):
        kg = self.doc.kg
        self.assertIn('@id', kg._kg)
        self.assertEqual('http://xxx/1', kg._kg['@id'])
        self.assertIn('@type', kg._kg)
        self.assertIn(DIG.Person.toPython(), kg._kg['@type'])

    def test_valid_kg(self):
        kg = self.doc2.kg
        self.assertNotIn('@id', kg._kg)
        self.assertNotIn('@type', kg._kg)

    def test_add_value_kg_jsonld(self):
        kg = self.doc.kg
        field_name = kg.context_resolve(DIG.has_name)
        self.assertEqual('has_name', field_name)
        kg.add_value(field_name, 'Jack')
        self.assertIn({'@value': 'Jack'}, kg._kg[field_name])
        field_child = kg.context_resolve(DIG.has_child)
        self.assertEqual('has_child', field_child)
        child1 = 'http://xxx/2'
        child2 = {'@id': 'http://xxx/3', 'has_name': 'Daniels', '@type': [DIG.Person],
                  '@context': {'has_name': DIG.has_name.toPython()}}
        kg.add_value(field_child, child1)
        kg.add_value(field_child, child2)
        self.assertIn({'@id': 'http://xxx/2'}, kg._kg[field_child])

    def test_add_value_kg(self):
        kg = self.doc2.kg

        field_name = kg.context_resolve(DIG.has_name)

        self.assertEqual('has_name', field_name)
        kg.add_value(field_name, 'Jack')
        self.assertIn({'value': 'Jack', "key": "jack"}, kg._kg[field_name])
