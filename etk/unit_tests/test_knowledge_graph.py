import unittest, json
from etk.knowledge_graph import KGSchema
from etk.etk import ETK
from etk.etk_exceptions import KgValueError
from datetime import date, datetime


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

        self.assertEqual(expected_developers, sample_doc.kg.value["developer"])
        self.assertEqual(expected_date, sample_doc.kg.value["test_date"])
        self.assertEqual(expected_location, sample_doc.kg.value["test_location"])
        self.assertEqual(expected_add_value_date, sample_doc.kg.value["test_add_value_date"])
