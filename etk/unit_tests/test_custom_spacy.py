# -*- coding: utf-8 -*-
import unittest
import sys, os

sys.path.append('../../')
from etk.core import Core
import json
import codecs


class TestExtractionsInputPaths(unittest.TestCase):
    def setUp(self):
        file_path = os.path.join(os.path.dirname(__file__), "ground_truth/custom_spacy.jl")
        self.doc = json.load(codecs.open(file_path))

    def test_extraction_input_path(self):
        spacy_rule_file_path = os.path.join(os.path.dirname(__file__), "resources/custom_spacy_rule.json")
        e_config = {
            "document_id": "doc_id",
            "extraction_policy": "replace",
            "error_handling": "raise_error",
            "resources": {
                "spacy_field_rules": {"JUNK": spacy_rule_file_path}
            },
            "content_extraction": {
                "input_path": "raw_content",
                "extractors": {
                    "readability": [
                        {
                            "strict": "yes",
                            "extraction_policy": "keep_existing",
                            "field_name": "content_strict"
                        },
                        {
                            "strict": "no",
                            "extraction_policy": "keep_existing",
                            "field_name": "content_relaxed"
                        }
                    ],
                    "title": {
                        "extraction_policy": "keep_existing"
                    }
                }
            },
            "data_extraction": [
                {
                    "input_path": [
                        "*.content_strict.text.`parent`",
                        "*.content_relaxed.text.`parent`",
                        "*.title.text.`parent`"
                    ],
                    "fields": {
                        "JUNK": {
                            "extractors": {
                                "extract_using_custom_spacy": {
                                    "config": {
                                        "spacy_field_rules": "JUNK"
                                    }
                                }
                            }
                        }
                    }
                }
            ]
        }
        c = Core(extraction_config=e_config)
        r = c.process(self.doc, create_knowledge_graph=True)
        # self.assertTrue('knowledge_graph' in r)
        if 'JUNK' in r['knowledge_graph']:
            custom_spacy_extracted = r['knowledge_graph']['JUNK']
        else:
            custom_spacy_extracted = []
        # print json.dumps(custom_spacy_extracted, indent=2)
        expected_extracted = [
            {
                "confidence": 1,
                "provenance": [
                    {
                        "source": {
                            "segment": "content_strict",
                            "context": {
                                "end": 31,
                                "text": "looking for an extraordinary experience <etk 'attribute' = 'JUNK'>Girl</etk> with an upscale lAdy who ",
                                "start": 30,
                                "input": "tokens",
                                "identifier": "rule_3",
                                "rule_id": 2
                            },
                            "document_id": "EA2C1F7B20F494D0ED3A7A39FBDB22F78584509E37EACA5198B2A05A6D9A8715"
                        },
                        "confidence": {
                            "extraction": 1.0
                        },
                        "method": "extract_using_custom_spacy",
                        "extracted_value": "Girl"
                    },
                    {
                        "source": {
                            "segment": "content_relaxed",
                            "context": {
                                "end": 40,
                                "text": "looking for an extraordinary experience <etk 'attribute' = 'JUNK'>Girl</etk> with an upscale lAdy who ",
                                "start": 39,
                                "input": "tokens",
                                "identifier": "rule_3",
                                "rule_id": 2
                            },
                            "document_id": "EA2C1F7B20F494D0ED3A7A39FBDB22F78584509E37EACA5198B2A05A6D9A8715"
                        },
                        "confidence": {
                            "extraction": 1.0
                        },
                        "method": "extract_using_custom_spacy",
                        "extracted_value": "Girl"
                    }
                ],
                "key": "girl",
                "value": "Girl"
            },
            {
                "confidence": 1,
                "provenance": [
                    {
                        "source": {
                            "segment": "content_strict",
                            "context": {
                                "end": 70,
                                "text": "call bA22by ! Confirmations \n\n <etk 'attribute' = 'JUNK'>i runqi \n\n\n 0321332</etk> Confirmations \n\n\n I ' ll ",
                                "start": 66,
                                "input": "tokens",
                                "identifier": "rule_0",
                                "rule_id": 0
                            },
                            "document_id": "EA2C1F7B20F494D0ED3A7A39FBDB22F78584509E37EACA5198B2A05A6D9A8715"
                        },
                        "confidence": {
                            "extraction": 1.0
                        },
                        "method": "extract_using_custom_spacy",
                        "extracted_value": "i runqi 0321332"
                    },
                    {
                        "source": {
                            "segment": "content_relaxed",
                            "context": {
                                "end": 79,
                                "text": "call bA22by ! Confirmations \n\n <etk 'attribute' = 'JUNK'>i runqi \n\n\n 0321332</etk> Confirmations \n\n\n I ' ll ",
                                "start": 75,
                                "input": "tokens",
                                "identifier": "rule_0",
                                "rule_id": 0
                            },
                            "document_id": "EA2C1F7B20F494D0ED3A7A39FBDB22F78584509E37EACA5198B2A05A6D9A8715"
                        },
                        "confidence": {
                            "extraction": 1.0
                        },
                        "method": "extract_using_custom_spacy",
                        "extracted_value": "i runqi 0321332"
                    }
                ],
                "key": "i runqi 0321332",
                "value": "i runqi 0321332"
            },
            {
                "confidence": 1,
                "provenance": [
                    {
                        "source": {
                            "segment": "content_relaxed",
                            "context": {
                                "end": 44,
                                "text": "experience Girl with an upscale <etk 'attribute' = 'JUNK'>lAdy</etk> who knows how to cater ",
                                "start": 43,
                                "input": "tokens",
                                "identifier": "rule_3",
                                "rule_id": 2
                            },
                            "document_id": "EA2C1F7B20F494D0ED3A7A39FBDB22F78584509E37EACA5198B2A05A6D9A8715"
                        },
                        "confidence": {
                            "extraction": 1.0
                        },
                        "method": "extract_using_custom_spacy",
                        "extracted_value": "lAdy"
                    }
                ],
                "key": "lady",
                "value": "lAdy"
            }
        ]
        self.assertEqual(expected_extracted, custom_spacy_extracted)


if __name__ == '__main__':
    unittest.main()
