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
        no_table_file = os.path.join(os.path.dirname(__file__), "ground_truth/1_content_extracted.jl")
        self.doc = json.load(codecs.open(file_path, "r", "utf-8"))
        self.table_ex = json.load(codecs.open(table_out, "r", "utf-8"))
        self.no_table = json.load(codecs.open(no_table_file, "r", "utf-8"))

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
        with open("table_out.jl", "w") as f:
            f.write(json.dumps(r["content_extraction"]["table"]))

        self.assertTrue("content_extraction" in r)
        self.assertTrue("table" in r["content_extraction"])
        ex = json.loads(json.JSONEncoder().encode(r["content_extraction"]["table"]))
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

        self.assertTrue("content_extraction" in r)
        self.assertTrue("table" in r["content_extraction"])
        ex = json.loads(json.JSONEncoder().encode(r["content_extraction"]["table"]))
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

        self.assertTrue("content_extraction" in r)
        self.assertTrue("table" in r["content_extraction"])
        ex = json.loads(json.JSONEncoder().encode(r["content_extraction"]["table"]))
        self.assertEqual(ex, self.table_ex)

    def test_table_extractor_no_table(self):
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
        r = c.process(self.no_table)
        # print r
        self.assertTrue("content_extraction" in r)
        self.assertTrue("table" in r["content_extraction"])
        self.assertEqual(r["content_extraction"]["table"], None)


if __name__ == '__main__':
    unittest.main()
