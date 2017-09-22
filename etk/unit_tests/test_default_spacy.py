import unittest
import sys
import os
import codecs

sys.path.append('../../')
from etk.core import Core
import json


class TestTableExtractions(unittest.TestCase):

    def setUp(self):
        self.e_config = json.load(open(os.path.join(os.path.dirname(
            __file__), 'resources/extraction_config_default_spacy.json'), 'r'))
        self.ground_truth_input = list()
        self.ground_truth_output = list()
        ground_truth_input_file = codecs.open(os.path.join(os.path.dirname(
            __file__), 'ground_truth/default_spacy.jl'), 'r', 'utf-8')
        ground_truth_output_file = codecs.open(os.path.join(os.path.dirname(
            __file__), 'ground_truth/default_spacy_output.jl'), 'r', 'utf-8')
        for row in ground_truth_input_file:
            self.ground_truth_input.append(json.loads(row))
        for row in ground_truth_output_file:
            self.ground_truth_output.append(json.loads(row))

    def test_extraction_from_default_spacy(self):
        c = Core(extraction_config=self.e_config, load_spacy=True)
        for i in range(len(self.ground_truth_input)):
            r = c.process(self.ground_truth_input[
                          i], create_knowledge_graph=True)
            self.assertEquals(self.ground_truth_output[i][
                              'knowledge_graph'], r['knowledge_graph'])

if __name__ == '__main__':
    unittest.main()
