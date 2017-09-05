# -*- coding: utf-8 -*-
import unittest
import sys, os

sys.path.append('../../')
from etk.core import Core
import json
import codecs
import pygtrie as trie


class TestExtractionsUsingDictionaries(unittest.TestCase):
    def setUp(self):
        file_path = os.path.join(os.path.dirname(__file__), "ground_truth/1_content_extracted.jl")
        self.doc = json.load(codecs.open(file_path))

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
        self.assertTrue("simple_tokens_original_case" in r["content_extraction"]["content_strict"])
        self.assertTrue("simple_tokens" in r["content_extraction"]["content_strict"])
        self.assertTrue("data_extraction" in r["content_extraction"]["content_strict"])
        self.assertTrue("name" in r["content_extraction"]["content_strict"]["data_extraction"])
        self.assertTrue(
            "extract_using_dictionary" in r["content_extraction"]["content_strict"]["data_extraction"]["name"])
        extraction = r["content_extraction"]["content_strict"]["data_extraction"]["name"]["extract_using_dictionary"]

        ex = {'results': [
            {'origin': {'score': 1.0, 'segment': 'content_strict', 'method': 'extract_using_dictionary'},
             'context': {'end': 11,
                         'text': "27 \n\n\n my name is <etk 'attribute' = 'name'>helena</etk> height 160cms "
                                 "weight 55 kilos ",
                         'start': 10, 'input': 'tokens'}, 'value': u'helena'},
            {'origin': {'score': 1.0, 'segment': 'content_strict', 'method': 'extract_using_dictionary'},
             'context': {'end': 137,
                         'text': "\n\n hey i ' m <etk 'attribute' = 'name'>luna</etk> 3234522013 let ' s explore ",
                         'start': 136, 'input': 'tokens'}, 'value': u'luna'}]}
        self.assertEqual(extraction, ex)

    def test_empty_tokens(self):
        tokens = []
        pre_process = lambda x: x
        pre_filter = lambda x: x
        post_filter = lambda x: isinstance(x, basestring)
        ngrams = 1
        joiner = ' '
        n_trie = trie.CharTrie()
        c = Core()
        r = c._extract_using_dictionary(tokens, pre_process, n_trie, pre_filter, post_filter,
                                        ngrams, joiner)
        self.assertEqual(r, None)

    def test_positive_case_sensitive(self):
        women_name_file_path = os.path.join(os.path.dirname(__file__), "resources/case_sensitive_female_name.json")
        doc = {
            'content_extraction': {
                'content_strict': {
                    'text': 'My name is Margie and this is a test for extracting this name using case sensitive '
                            'dictionary'
                }
            },
            'doc_id': 'id',
            'url': 'http://givemeabreak.com'
        }
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
                                        'case_sensitive': 'True',
                                        "dictionary": "women_name",
                                        "ngrams": 1,
                                        "joiner": " ",
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
        r = c.process(doc)

        self.assertTrue("simple_tokens" in r["content_extraction"]["content_strict"])
        self.assertTrue("data_extraction" in r["content_extraction"]["content_strict"])
        self.assertTrue('simple_tokens_original_case' in r["content_extraction"]["content_strict"])
        self.assertTrue("name" in r["content_extraction"]["content_strict"]["data_extraction"])
        self.assertTrue(
            "extract_using_dictionary" in r["content_extraction"]["content_strict"]["data_extraction"]["name"])
        extraction = r["content_extraction"]["content_strict"]["data_extraction"]["name"]["extract_using_dictionary"]
        expected_extraction = {
                  "results": [
                    {
                      "origin": {
                        "score": 1.0,
                        "segment": "content_strict",
                        "method": "extract_using_dictionary"
                      },
                      "context": {
                        "end": 4,
                        "text": "My name is <etk 'attribute' = 'name'>Margie</etk> and this is a test ",
                        "start": 3,
                        "input": "tokens"
                      },
                      "value": "Margie"
                    }
                  ]
                }
        self.assertEqual(extraction, expected_extraction)

    def test_negative_case_sensitive(self):
        women_name_file_path = os.path.join(os.path.dirname(__file__), "resources/case_sensitive_female_name.json")
        doc = {
            'content_extraction': {
                'content_strict': {
                    'text': 'My name is margie and this is a test for extracting this name using case sensitive '
                            'dictionary'
                }
            },
            'doc_id': 'id',
            'url': 'http://givemeabreak.com'
        }
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
                                        'case_sensitive': 'trUe',
                                        "dictionary": "women_name",
                                        "ngrams": 1,
                                        "joiner": " ",
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
        r = c.process(doc)

        self.assertTrue("simple_tokens" in r["content_extraction"]["content_strict"])
        self.assertTrue('simple_tokens_original_case' in r["content_extraction"]["content_strict"])
        self.assertTrue("data_extraction" not in r["content_extraction"]["content_strict"])

if __name__ == '__main__':
    unittest.main()
