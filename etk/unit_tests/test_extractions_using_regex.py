# -*- coding: utf-8 -*-
import unittest
import sys, os

sys.path.append('../../')
from etk.core import Core
import json
import codecs


class TestExtractionsUsingRegex(unittest.TestCase):
    def setUp(self):
        file_path = os.path.join(os.path.dirname(__file__), "ground_truth/1_content_extracted.jl")
        self.doc = json.load(codecs.open(file_path))

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
            "document_id": "doc_id",
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
        self.assertTrue("simple_tokens" in r["content_extraction"]["content_strict"])
        self.assertTrue("simple_tokens_original_case" in r["content_extraction"]["content_strict"])
        self.assertTrue("knowledge_graph" in r)
        self.assertTrue("name" in r['knowledge_graph'])
        self.assertTrue(len(r['knowledge_graph']["name"]) == 1)
        extraction = r['knowledge_graph']["name"][0]
        ex = {
            "confidence": 1,
            "provenance": [
                {
                    "source": {
                        "segment": "content_strict",
                        "context": {
                            "start": 41,
                            "end": 58,
                            "input": "text",
                            "text": "91  27  \n  <etk 'attribute' = 'name'>My name is Helena</etk>  height 16"
                        },
                        "document_id": "1A4A5FF5BD066309C72C8EEE6F7BCCCFD21B83245AFCDADDF014455BCF990A21"
                    },
                    "confidence": {
                        "extraction": 1.0
                    },
                    "method": "extract_using_regex",
                    "extracted_value": "Helena"
                }
            ],
            "key": "helena",
            "value": "Helena"
        }
        self.assertEqual(extraction, ex)


if __name__ == '__main__':
    unittest.main()
