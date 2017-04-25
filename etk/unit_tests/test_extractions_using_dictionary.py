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
        self.assertTrue("tokens" in r["content_extraction"]["content_strict"])
        self.assertTrue("simple_tokens" in r["content_extraction"]["content_strict"])
        self.assertTrue("data_extraction" in r["content_extraction"]["content_strict"])
        self.assertTrue("name" in r["content_extraction"]["content_strict"]["data_extraction"])
        self.assertTrue(
            "extract_using_dictionary" in r["content_extraction"]["content_strict"]["data_extraction"]["name"])
        extraction = r["content_extraction"]["content_strict"]["data_extraction"]["name"]["extract_using_dictionary"]

        ex = {'results': [
            {'origin': {'score': 1.0, 'segment': 'readability_strict', 'method': 'extract_using_dictionary'},
             'context': {'end': 11, 'tokens_left': [u'27', u'\n\n\n', u'my', u'name', u'is'],
                         'text': u'27 \n\n\n my name is helena height 160cms weight 55 kilos', 'start': 10,
                         'input': 'tokens', 'tokens_right': [u'height', u'160cms', u'weight', u'55', u'kilos']},
             'value': u'helena'},
            {'origin': {'score': 1.0, 'segment': 'readability_strict', 'method': 'extract_using_dictionary'},
             'context': {'end': 137, 'tokens_left': [u'\n\n', u'hey', u'i', u"'", u'm'],
                         'text': u"\n\n hey i ' m luna 3234522013 let ' s explore", 'start': 136, 'input': 'tokens',
                         'tokens_right': [u'3234522013', u'let', u"'", u's', u'explore']}, 'value': u'luna'}]}
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


if __name__ == '__main__':
    unittest.main()
