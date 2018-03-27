import unittest
from etk.knowledge_graph import KnowledgeGraph, KgSchema
from etk.etk import ETK


class TestKnowledgeGraph(unittest.TestCase):

    def test_KnowledgeGraph(self) -> None:
        sample_doc = {
          "projects": [
            {
              "name": "etk",
              "description": "version 2 of etk, implemented by Runqi12 Shao, Dongyu Li, Sylvia lin, Amandeep and others.",
              "members": [
                "dongyu",
                "amandeep",
                "sylvia"
              ]
            },
            {
              "name": "rltk",
              "description": "record linkage toolkit, implemented by Pedro, Mayank, Yixiang and several students.",
              "members": [
                "mayank",
                "yixiang",
                "pedro"
              ]
            }
          ]
        }

        master_config = {
            "fields": {
                "developer": {
                    "type": "string"
                },
            }
        }

        etk = ETK()
        doc = etk.create_document(sample_doc)
        kg_schema = KgSchema(master_config)

        knowledge_graph = KnowledgeGraph(kg_schema, doc, etk)
        knowledge_graph.add_value("developer", "projects[*].members[*]")
        expected = {
          "developer": [
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
              "value": "mayank",
              "key": "mayank"
            },
            {
              "value": "yixiang",
              "key": "yixiang"
            },
            {
              "value": "pedro",
              "key": "pedro"
            }
          ]
        }

        self.assertEqual(expected, knowledge_graph.get_kg())