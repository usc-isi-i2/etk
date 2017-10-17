import unittest
import sys
import os

sys.path.append('../../../')
from etk.core import Core
import json


class TestTableExtractions(unittest.TestCase):
    def setUp(self):
        self.infile = open(os.path.join(os.path.dirname(__file__), "../table_data_extraction/test_table.jl"))
        self.e_config = json.load(
            open(os.path.join(os.path.dirname(__file__), "../resources/extraction_config_table.json")))

    def test_extraction_from_table(self):
        c = Core(extraction_config=self.e_config)
        doc = json.loads(self.infile.read())
        r = c.process(doc, create_knowledge_graph=True)
        self.assertEqual(r['knowledge_graph']['age'][0]['provenance'][0]['method'], 'table_data_extractor')


if __name__ == '__main__':
    unittest.main()
