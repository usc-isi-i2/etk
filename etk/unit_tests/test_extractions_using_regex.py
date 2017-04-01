# -*- coding: utf-8 -*-
import unittest
import sys
sys.path.append('../../')
from etk.core import Core
import json
import codecs


class TestExtractionsUsingDictionaries(unittest.TestCase):

    def setUp(self):
        self.doc = json.load(codecs.open('ground_truth/1_content_extracted.jl'))

    def test_extractor__no_regex(self):
        e_config = {
            "data_extraction": [
                {
                    "input_path": ["content_extraction.content_strict.text.`parent`"]
                    ,
                    "fields": {
                        "name": {
                            "extractors": {
                                "extract_using_regex": {
                                    "config": {
                                        "include_context": "true",
                                        "regex_options": [
                                            "IGNORECASE"
                                        ],
                                        "pre_filter": [
                                            "x.replace('\\n', '')",
                                            "x.replace('\\r', '')"
                                        ]
                                    },
                                    "extraction_policy": "replace"
                                }
                            }

                        }
                    }
                }
            ]
        }
        c = Core(extraction_config=e_config)
        with self.assertRaises(KeyError):
            r = c.process(self.doc)

    def test_extractor_regex(self):
        e_config = {
            "data_extraction": [
                {
                    "input_path": ["content_extraction.content_strict.text.`parent`"]
                    ,
                    "fields": {
                        "name": {
                            "extractors": {
                                "extract_using_regex": {
                                    "config": {
                                        "include_context": "true",
                                        "regex": "(?:my[\\s]+name[\\s]+is[\\s]+([-a-z0-9@$!]+))",
                                        "regex_options": [
                                            "IGNORECASE"
                                        ],
                                        "pre_filter": [
                                            "x.replace('\\n', '')",
                                            "x.replace('\\r', '')"
                                        ]
                                    },
                                    "extraction_policy": "replace"
                                }
                            }

                        }
                    }
                }
            ]
        }
        c = Core(extraction_config=e_config)
        r = c.process(self.doc)
        self.assertTrue("content_extraction" in r)
        self.assertTrue("content_strict" in r["content_extraction"])
        self.assertTrue("text" in r["content_extraction"]["content_strict"])
        self.assertTrue("tokens" in r["content_extraction"]["content_strict"])
        self.assertTrue("simple_tokens" in r["content_extraction"]["content_strict"])
        self.assertTrue("data_extraction" in r["content_extraction"]["content_strict"])
        self.assertTrue("name" in r["content_extraction"]["content_strict"]["data_extraction"])
        self.assertTrue("extract_using_regex" in r["content_extraction"]["content_strict"]["data_extraction"]["name"])
        extraction = r["content_extraction"]["content_strict"]["data_extraction"]["name"]["extract_using_regex"]
        ex = {
            "results": [
                {
                    "origin": {
                        "score": 1,
                        "segment": "readability_strict",
                        "method": "other_method"
                    },
                    "context": {
                        "field": "text",
                        "end": 73,
                        "start": 56
                    },
                    "value": "Helena"
                }
            ]
        }
        self.assertEqual(extraction, ex)


if __name__ == '__main__':
    unittest.main()
