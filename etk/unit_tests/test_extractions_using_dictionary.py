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

    def test_extractor_dictionary_no_resources(self):
        e_config = {
            "resources": {
                "dictionaries": {
                }
            },
            "data_extraction": [
                {
                    "input_path": "content_extraction.content_strict.text.`parent`"
                    ,
                    "fields": {
                        "name": {
                            "extractors": {
                                "extract_using_dictionary": {
                                    "config": {
                                        "dictionary": "women_name",
                                        "ngrams": 1,
                                        "joiner": " ",
                                        "pre_process": [
                                            "x.lower()"
                                        ],
                                        "pre_filter": [
                                            "x"
                                        ],
                                        "post_filter": [
                                            "isinstance(x, basestring)"
                                        ]
                                    },
                                    "extraction_policy": "keep_existing"
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
            self.assertTrue("content_extraction" in r)
            self.assertTrue("content_strict" in r["content_extraction"])
            self.assertTrue("text" in r["content_extraction"]["content_strict"])
            self.assertTrue("tokens" in r["content_extraction"]["content_strict"])
            self.assertTrue("simple_tokens" in r["content_extraction"]["content_strict"])

    def test_extractor_dictionary(self):
        e_config = {
            "resources": {
                "dictionaries": {
                    "women_name": "resources/female-names.json.gz"
                }
            },
            "data_extraction": [
                {
                    "input_path": "content_extraction.content_strict.text.`parent`"
                    ,
                    "fields": {
                        "name": {
                            "extractors": {
                                "extract_using_dictionary": {
                                    "config": {
                                        "dictionary": "women_name",
                                        "ngrams": 1,
                                        "joiner": " ",
                                        "pre_process": [
                                            "x.lower()"
                                        ],
                                        "pre_filter": [
                                            "x"
                                        ],
                                        "post_filter": [
                                            "isinstance(x, basestring)"
                                        ]
                                    },
                                    "extraction_policy": "keep_existing"
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
        self.assertTrue(
            "extract_using_dictionary" in r["content_extraction"]["content_strict"]["data_extraction"]["name"])
        extraction = r["content_extraction"]["content_strict"]["data_extraction"]["name"]["extract_using_dictionary"]
        ex = {"results": [
            {
                "origin": {
                    "score": 1,
                    "segment": "readability_strict",
                    "method": "other_method"
                },
                "context": {
                    "field": "tokens",
                    "end": 11,
                    "start": 10
                },
                "value": "helena"
            }]}
        self.assertEqual(extraction, ex)

if __name__ == '__main__':
    unittest.main()
