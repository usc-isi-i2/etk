import unittest
import sys
import os
sys.path.append('../../')
from etk.core import Core
import json
import codecs


class TestNestedDocs(unittest.TestCase):

    def setUp(self):
        self.e_config = {
            "document_id": "doc_id",
            "extraction_policy": "replace",
            "error_handling": "raise_error",
            "resources": {
                "dictionaries": {

                },
                "landmark": [
                ],
                "pickle": {
                }
            },
            "content_extraction": {
                "input_path": "raw_content",
                "extractors": {
                    "table": {
                        "field_name": "table",
                        "config": {
                        },
                        "extraction_policy": "replace"
                    }
                }
            },
            "data_extraction": [
                {
                  "input_path": [
                    "*.table.tables[*].rows[*]"
                  ],
                  "fields": {
                    "nested_test": {
                      "extractors": {
                        "create_kg_node_extractor": {
                          "config": {
                            "@type": "table_row_doc",
                            "segment_name": "table_row"
                          }
                        }
                      }
                    }
                  }
                }
            ],
            "kg_enhancement": [

            ]
        }
        file_path = os.path.join(os.path.dirname(__file__), "ground_truth/nested_doc.jl")
        self.doc = json.load(codecs.open(file_path, "r", "utf-8"))

    def test_num_nested_docs(self):
        c = Core(extraction_config=self.e_config)
        r = c.process(self.doc)
        self.assertTrue("nested_docs" in r)
        self.assertEqual(len(r["nested_docs"]), 78)

if __name__ == '__main__':
    unittest.main()
