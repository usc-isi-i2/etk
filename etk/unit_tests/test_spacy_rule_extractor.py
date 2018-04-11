import unittest
from etk.extractors.spacy_rule_extractor import SpacyRuleExtractor
import spacy


class TestSpacyRuleExtractor(unittest.TestCase):

    def test_SpacyRuleExtractor(self) -> None:
        sample_rules = {
            "field_name": "test",
            "rules": [
              {
                "dependencies": [],
                "description": "",
                "identifier": "rule_3",
                "is_active": "true",
                "output_format": "firstName:{1}, lastName:{2}",
                "pattern": [
                  {
                    "capitalization": ["title"],
                    "contain_digit": "true",
                    "is_in_output": "true",
                    "is_in_vocabulary": "false",
                    "is_out_of_vocabulary": "false",
                    "is_required": "true",
                    "length": [],
                    "match_all_forms": "true",
                    "maximum": "",
                    "minimum": "",
                    "numbers": [],
                    "part_of_speech": [],
                    "prefix": "",
                    "shapes": [
                    ],
                    "suffix": "",
                    "token": [
                    ],
                    "type": "word"
                  },
                  {
                    "capitalization": ["title"],
                    "contain_digit": "false",
                    "is_in_output": "true",
                    "is_in_vocabulary": "false",
                    "is_out_of_vocabulary": "false",
                    "is_required": "false",
                    "length": [],
                    "match_all_forms": "true",
                    "maximum": "",
                    "minimum": "",
                    "numbers": [],
                    "part_of_speech": [],
                    "prefix": "",
                    "shapes": [
                    ],
                    "suffix": "",
                    "token": [
                    ],
                    "type": "word"
                  }
                ],
                "polarity": "true"
              },
              {
                "dependencies": [],
                "description": "",
                "identifier": "rule_4",
                "is_active": "true",
                "output_format": "number:{1}",
                "pattern": [
                  {
                    "capitalization": [],
                    "contain_digit": "true",
                    "is_in_output": "true",
                    "is_in_vocabulary": "false",
                    "is_out_of_vocabulary": "false",
                    "is_required": "true",
                    "length": [],
                    "match_all_forms": "true",
                    "maximum": "5",
                    "minimum": "0",
                    "numbers": [],
                    "part_of_speech": [],
                    "prefix": "",
                    "shapes": [
                    ],
                    "suffix": "",
                    "token": [
                    ],
                    "type": "number"
                  }
                ],
                "polarity": "true"
              }
            ]
        }
        sample_rule_extractor = SpacyRuleExtractor(spacy.load("en_core_web_sm"), sample_rules, "test_extractor")
        extractions = sample_rule_extractor.extract(
          "version 2 of etk, implemented by Runqi12 Shao, Dongyu Li, Sylvia lin, Amandeep and others.")

        expected = [('rule_4', 'number:2'), ('rule_3', 'firstName:Runqi12, lastName:Shao'),
                    ('rule_3', 'firstName:Dongyu, lastName:Li'), ('rule_3', 'firstName:Sylvia, lastName:{2}'),
                    ('rule_3', 'firstName:Amandeep, lastName:{2}')]
        self.assertEqual([(x.rule_id, x.value) for x in extractions], expected)

if __name__ == '__main__':
    unittest.main()