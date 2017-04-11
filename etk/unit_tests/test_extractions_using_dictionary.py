# -*- coding: utf-8 -*-
import unittest
import sys, os
sys.path.append('../../')
from etk.core import Core
import json
import codecs


class TestExtractionsUsingDictionaries(unittest.TestCase):

    def setUp(self):
        file_path = os.path.join(os.path.dirname(__file__), "ground_truth/1_content_extracted.jl")
        self.doc = json.load(codecs.open(file_path))

    def test_extractor_dictionary_no_resources(self):
        print "extractions_using_dictionary.extractor_dictionary_no_resources"
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
        print "extractions_using_dictionary.extractor_dictionary"
        women_name_file_path = os.path.join(os.path.dirname(__file__), "resources/female-names.json.gz")
        e_config = {
            "resources": {
                "dictionaries": {
                    "women_name": women_name_file_path
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
                    'text': u'my name is helena height 160cms weight',
                    "end": 11,
                    "start": 10
                },
                "value": "helena"
            },
            {'origin': {'score': 1.0, 'segment': 'readability_strict', 'method': 'other_method'},
             'context': {'text': u"i ' m luna 3234522013 let '", 'end': 137, 'start': 136}, 'value': u'luna'}]}
        self.assertEqual(extraction, ex)

if __name__ == '__main__':
    unittest.main()
