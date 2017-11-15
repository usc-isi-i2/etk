# -*- coding: utf-8 -*-
import unittest
import sys, os

sys.path.append('../../')
from etk.core import Core
import json
import codecs

class TestExtractionsInputPaths(unittest.TestCase):
    def setUp(self):
        file_path = os.path.join(os.path.dirname(__file__), "resources/infer_rule_input.json")
        self.obj = json.load(codecs.open(file_path))

    def test_extraction_input_path(self):
        c = Core()
        d = dict()
        d['simple_tokens_original_case'] = c.extract_tokens_from_crf(
            c.extract_crftokens(self.obj['test_text'], lowercase=False))

        p_filtered = [[x.decode('string_escape').decode("utf-8") for x in pp if x] for pp in
                      self.obj['positive_examples']]
        infered_rule = c.infer_rule_using_custom_spacy(d, p_filtered)
        expected_result =[
            {
                "polarity": "true",
                "description": "",
                "pattern": [
                    {
                        "shapes": [],
                        "prefix": "",
                        "is_in_output": "true",
                        "capitalization": [],
                        "part_of_speech": [],
                        "maximum": "",
                        "match_all_forms": "true",
                        "length": [],
                        "minimum": "",
                        "numbers": [],
                        "contain_digit": "true",
                        "is_in_vocabulary": "",
                        "is_out_of_vocabulary": "",
                        "is_required": "false",
                        "type": "punctuation",
                        "token": [
                            ";"
                        ],
                        "suffix": ""
                    }
                ],
                "output_format": "",
                "is_active": "true",
                "dependencies": [],
                "identifier": "infer_rule"
            },
            {
                "polarity": "true",
                "description": "",
                "pattern": [
                    {
                        "shapes": [],
                        "prefix": "",
                        "is_in_output": "true",
                        "capitalization": [],
                        "part_of_speech": [],
                        "maximum": "",
                        "match_all_forms": "true",
                        "length": [],
                        "minimum": "",
                        "numbers": [],
                        "contain_digit": "true",
                        "is_in_vocabulary": "",
                        "is_out_of_vocabulary": "",
                        "is_required": "true",
                        "type": "punctuation",
                        "token": [
                            ";"
                        ],
                        "suffix": ""
                    }
                ],
                "output_format": "",
                "is_active": "true",
                "dependencies": [],
                "identifier": "infer_rule"
            }
        ]
        self.assertIn(infered_rule, expected_result)


if __name__ == '__main__':
    unittest.main()
