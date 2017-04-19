import unittest
import sys
import os
sys.path.append('../../')
from etk.core import Core
import json
import codecs


class TestTableExtractions(unittest.TestCase):

    def setUp(self):
        file_path = os.path.join(os.path.dirname(__file__), "ground_truth/table.jl")
        table_out = os.path.join(os.path.dirname(__file__), "ground_truth/table_out.jl")
        self.doc = json.load(codecs.open(file_path, "r", "utf-8"))
        self.table_ex = json.load(codecs.open(table_out, "r", "utf-8"))

    def test_table_extractor(self):
        e_config = {'content_extraction': {
            "input_path": "raw_content",
            "extractors": {
                "table": {
                    "field_name": "table",
                    "extraction_policy": "keep_existing"
                }
            }
        }
        }
        c = Core(extraction_config=e_config)
        r = c.process(self.doc)
        ex = json.loads(json.JSONEncoder().encode(r["content_extraction"]["table"]))

        self.assertTrue("content_extraction" in r)
        self.assertTrue("table" in r["content_extraction"])
        self.assertEqual(ex, self.table_ex)

    def test_table_extractor_no_field_name(self):
        e_config = {'content_extraction': {
            "input_path": "raw_content",
            "extractors": {
                "table": {
                    "extraction_policy": "keep_existing"
                }
            }
        }
        }
        c = Core(extraction_config=e_config)
        r = c.process(self.doc)
        ex = json.loads(json.JSONEncoder().encode(r["content_extraction"]["table"]))

        self.assertTrue("content_extraction" in r)
        self.assertTrue("table" in r["content_extraction"])
        self.assertEqual(ex, self.table_ex)

    def test_table_extractor_empty_config(self):
        e_config = {'content_extraction': {
            "input_path": "raw_content",
            "extractors": {
                "table": {
                }
            }
        }
        }
        c = Core(extraction_config=e_config)
        r = c.process(self.doc)
        ex = json.loads(json.JSONEncoder().encode(r["content_extraction"]["table"]))

        self.assertTrue("content_extraction" in r)
        self.assertTrue("table" in r["content_extraction"])
        self.assertEqual(ex, self.table_ex)

if __name__ == '__main__':
    unittest.main()
