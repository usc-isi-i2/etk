import unittest
from etk.knowledge_graph import KGSchema
from etk.etk import ETK
from etk.etk_exceptions import KgValueInvalidError
from datetime import date, datetime


class TestKnowledgeGraph(unittest.TestCase):

    def test_KnowledgeGraph(self) -> None:
        sample_doc = {
          "projects": [
            {
              "name": "etk",
              "description": "version 2 of etk, implemented by Runqi12 Shao, Dongyu Li, Sylvia lin, Amandeep and others.",
              "members": [
                "dongyu",
                -32.1,
                "amandeep",
                "sylvia",
                "Runqi12"
              ],
              "date": "2007-12-05",
              "place": "columbus:georgia:united states:-84.98771:32.46098"
            },
            {
              "name": "rltk",
              "description": "record linkage toolkit, implemented by Pedro, Mayank, Yixiang and several students.",
              "members": [
                "mayank",
                "yixiang",
                12
              ],
              "date": ["2007-12-05T23:19:00"],
              "cost": -3213.32
            }
          ]
        }

        master_config = {
            "fields": {
                "developer": {
                    "type": "string"
                },
                "test_date": {
                    "type": "date"
                },
                "test_location": {
                    "type": "location"
                },
                "test_number": {
                    "type": "number"
                },
                "test_add_value_date": {
                    "type": "date"
                }
            }
        }

        kg_schema = KGSchema(master_config)

        etk = ETK(kg_schema)
        doc = etk.create_document(sample_doc)

        try:
            doc.kg.add_doc_value("developer", "projects[*].members[*]")
        except KgValueInvalidError:
            pass

        try:
            doc.kg.add_doc_value("test_date", "projects[*].date[*]")
        except KgValueInvalidError:
            pass

        try:
            doc.kg.add_value("test_add_value_date", [date(2018,3,28), {}, datetime(2018,3,28, 1,1,1)])
        except KgValueInvalidError:
            pass

        try:
            doc.kg.add_doc_value("test_location", "projects[*].place")
        except KgValueInvalidError:
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
              "value": "2018-03-28T00:00:00",
              "key": "2018-03-28T00:00:00"
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
        self.assertEqual(expected_developers, doc.kg.value["developer"])
        self.assertEqual(expected_date, doc.kg.value["test_date"])
        self.assertEqual(expected_location, doc.kg.value["test_location"])
        self.assertEqual(expected_add_value_date, doc.kg.value["test_add_value_date"])
