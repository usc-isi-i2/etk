import unittest, json
from etk.knowledge_graph import KGSchema
from etk.etk import ETK
from etk.etk_exceptions import KgValueError
from datetime import date, datetime
from etk.provenance_api import ProvenanceAPI


class TestKnowledgeGraphProvenance(unittest.TestCase):

    def test_KnowledgeGraph_provenance(self) -> None:
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
                    "place": "columbus:georgia:united states:-84.98771:32.46098"
                },
                {
                    "name": "rltk",
                    "description": "record linkage toolkit, implemented by Pedro, Mayank, Yixiang and several students.",
                    "members": [
                        "mayank",
                        "yixiang"
                    ],
                    "date": ["2007-12-05T23:19:00"],
                    "cost": -3213.32
                }
            ]
        }

        kg_schema = KGSchema(json.load(open('etk/unit_tests/ground_truth/test_config.json')))

        etk = ETK(kg_schema)
        doc = etk.create_document(sample_doc)

        try:
            doc.kg.add_value("developer", json_path="projects[*].members[*]")
        except KgValueError:
            pass

        try:
            doc.kg.add_value("test_date", json_path="projects[*].date[*]")
        except KgValueError:
            pass

        try:
            doc.kg.add_value("test_add_value_date", value=[date(2018, 3, 28), {}, datetime(2018, 3, 28, 1, 1, 1)],
                             json_path_extraction="projects[0].date")
        except KgValueError:
            pass

        try:
            doc.kg.add_value("test_location", json_path="projects[*].place")
        except KgValueError:
            pass

        # print (json.dumps(doc.value, indent=2))

        expeced_provenances = [
            {
                "@id": 0,
                "@type": "kg_provenance_record",
                "reference_type": "location",
                "value": "dongyu",
                "json_path": "projects.[0].members.[0]"
            },
            {
                "@id": 1,
                "@type": "kg_provenance_record",
                "reference_type": "location",
                "value": "amandeep",
                "json_path": "projects.[0].members.[1]"
            },
            {
                "@id": 2,
                "@type": "kg_provenance_record",
                "reference_type": "location",
                "value": "sylvia",
                "json_path": "projects.[0].members.[2]"
            },
            {
                "@id": 3,
                "@type": "kg_provenance_record",
                "reference_type": "location",
                "value": "Runqi12",
                "json_path": "projects.[0].members.[3]"
            },
            {
                "@id": 4,
                "@type": "kg_provenance_record",
                "reference_type": "location",
                "value": "mayank",
                "json_path": "projects.[1].members.[0]"
            },
            {
                "@id": 5,
                "@type": "kg_provenance_record",
                "reference_type": "location",
                "value": "yixiang",
                "json_path": "projects.[1].members.[1]"
            },
            {
                "@id": 6,
                "@type": "kg_provenance_record",
                "reference_type": "location",
                "value": "2007-12-05T00:00:00",
                "json_path": "projects.[0].date.[0]"
            },
            {
                "@id": 7,
                "@type": "kg_provenance_record",
                "reference_type": "location",
                "value": "2007-12-05T23:19:00",
                "json_path": "projects.[1].date.[0]"
            },
            {
                "@id": 8,
                "@type": "kg_provenance_record",
                "reference_type": "constant",
                "value": "2018-03-28",
                "json_path": "projects[0].date"
            },
            {
                "@id": 9,
                "@type": "kg_provenance_record",
                "reference_type": "constant",
                "value": "2018-03-28T01:01:01",
                "json_path": "projects[0].date"
            },
            {
                "@id": 10,
                "@type": "kg_provenance_record",
                "reference_type": "location",
                "value": "columbus:georgia:united states:-84.98771:32.46098",
                "json_path": "projects.[0].place"
            }
        ]

        self.assertEqual(expeced_provenances, doc.value["provenances"])
