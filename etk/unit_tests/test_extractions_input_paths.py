# -*- coding: utf-8 -*-
import unittest
import sys

sys.path.append('../../')
from etk.core import Core
import json
import codecs


class TestExtractionsInputPaths(unittest.TestCase):
    def setUp(self):
        self.doc = json.load(codecs.open('ground_truth/1_content_extracted.jl'))

    def test_extraction_input_path(self):
        e_config = {
            "resources": {
                "dictionaries": {
                    "women_name": "resources/female-names.json.gz"
                }
            },
            "data_extraction": [
                {
                    "input_path": "*.*.text.`parent`"
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
                                },
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

        de_cs = r["content_extraction"]["content_strict"]["data_extraction"]["name"]
        self.assertTrue("extract_using_dictionary" in de_cs)
        eud = de_cs["extract_using_dictionary"]
        ex_eud = {
            "results": [
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
                },
                {
                    "origin": {
                        "score": 1,
                        "segment": "readability_strict",
                        "method": "other_method"
                    },
                    "context": {
                        "field": "tokens",
                        "end": 137,
                        "start": 136
                    },
                    "value": "luna"
                }
            ]
        }

        self.assertTrue("extract_using_regex" in de_cs)
        eur = de_cs["extract_using_regex"]
        ex_eur = {
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

        self.assertEqual(eur, ex_eur)

        self.assertTrue("content_extraction" in r)
        self.assertTrue("content_relaxed" in r["content_extraction"])
        self.assertTrue("text" in r["content_extraction"]["content_relaxed"])
        self.assertTrue("tokens" in r["content_extraction"]["content_relaxed"])
        self.assertTrue("simple_tokens" in r["content_extraction"]["content_relaxed"])
        self.assertTrue("data_extraction" in r["content_extraction"]["content_relaxed"])
        self.assertTrue("name" in r["content_extraction"]["content_relaxed"]["data_extraction"])

        de_cr = r["content_extraction"]["content_relaxed"]["data_extraction"]["name"]
        self.assertTrue("extract_using_dictionary" in de_cr)
        eudr = de_cr["extract_using_dictionary"]
        ex_eudr = {
            "results": [
                {
                    "origin": {
                        "score": 1,
                        "segment": "other_segment",
                        "method": "other_method"
                    },
                    "context": {
                        "field": "tokens",
                        "end": 11,
                        "start": 10
                    },
                    "value": "helena"
                },
                {
                    "origin": {
                        "score": 1,
                        "segment": "other_segment",
                        "method": "other_method"
                    },
                    "context": {
                        "field": "tokens",
                        "end": 137,
                        "start": 136
                    },
                    "value": "luna"
                }
            ]
        }
        self.assertEqual(eudr, ex_eudr)

        self.assertTrue("extract_using_regex" in de_cr)
        eurr = de_cr["extract_using_regex"]
        ex_eurr = {
            "results": [
                {
                    "origin": {
                        "score": 1,
                        "segment": "other_segment",
                        "method": "other_method"
                    },
                    "context": {
                        "field": "text",
                        "end": 75,
                        "start": 58
                    },
                    "value": "Helena"
                }
            ]
        }

        self.assertEqual(eurr, ex_eurr)

        self.assertTrue("content_extraction" in r)
        self.assertTrue("title" in r["content_extraction"])
        self.assertTrue("text" in r["content_extraction"]["title"])
        self.assertTrue("tokens" in r["content_extraction"]["title"])
        self.assertTrue("simple_tokens" in r["content_extraction"]["title"])


if __name__ == '__main__':
    unittest.main()
