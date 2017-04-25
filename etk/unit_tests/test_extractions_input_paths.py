# -*- coding: utf-8 -*-
import unittest
import sys, os

sys.path.append('../../')
from etk.core import Core
import json
import codecs


class TestExtractionsInputPaths(unittest.TestCase):
    def setUp(self):
        file_path = os.path.join(os.path.dirname(__file__), "ground_truth/1_content_extracted.jl")
        self.doc = json.load(codecs.open(file_path))

    def test_extraction_input_path(self):
        women_name_file_path = os.path.join(os.path.dirname(__file__), "resources/female-names.json.gz")
        e_config = {
            "resources": {
                "dictionaries": {
                    "women_name": women_name_file_path
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
        ex_eud = {'results': [
            {'origin': {'score': 1.0, 'segment': 'readability_strict', 'method': 'extract_using_dictionary'},
             'context': {'end': 11, 'tokens_left': [u'27', u'\n\n\n', u'my', u'name', u'is'],
                         'text': u'27 \n\n\n my name is helena height 160cms weight 55 kilos', 'start': 10,
                         'input': 'tokens', 'tokens_right': [u'height', u'160cms', u'weight', u'55', u'kilos']},
             'value': u'helena'},
            {'origin': {'score': 1.0, 'segment': 'readability_strict', 'method': 'extract_using_dictionary'},
             'context': {'end': 137, 'tokens_left': [u'\n\n', u'hey', u'i', u"'", u'm'],
                         'text': u"\n\n hey i ' m luna 3234522013 let ' s explore", 'start': 136, 'input': 'tokens',
                         'tokens_right': [u'3234522013', u'let', u"'", u's', u'explore']}, 'value': u'luna'}]}

        self.assertEqual(eud, ex_eud)

        self.assertTrue("extract_using_regex" in de_cs)
        eur = de_cs["extract_using_regex"]
        ex_eur = {'results': [
            {'origin': {'score': 1.0, 'segment': 'readability_strict', 'method': 'extract_using_regex'},
             'context': {'start': 56, 'end': 73, 'input': 'text', 'text': u' 27 \n \n \n My name is Helena height 16'},
             'value': u'Helena'}]}

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

        ex_eudr = {'results': [
            {'origin': {'score': 1.0, 'segment': 'readability_relaxed', 'method': 'extract_using_dictionary'},
             'context': {'end': 11, 'tokens_left': [u'27', u'\n\n\n', u'my', u'name', u'is'],
                         'text': u'27 \n\n\n my name is helena height 160cms weight 55 kilos', 'start': 10,
                         'input': 'tokens', 'tokens_right': [u'height', u'160cms', u'weight', u'55', u'kilos']},
             'value': u'helena'},
            {'origin': {'score': 1.0, 'segment': 'readability_relaxed', 'method': 'extract_using_dictionary'},
             'context': {'end': 137, 'tokens_left': [u'\n\n', u'hey', u'i', u"'", u'm'],
                         'text': u"\n\n hey i ' m luna 3234522013 let ' s explore", 'start': 136, 'input': 'tokens',
                         'tokens_right': [u'3234522013', u'let', u"'", u's', u'explore']}, 'value': u'luna'}]}
        self.assertEqual(eudr, ex_eudr)

        self.assertTrue("extract_using_regex" in de_cr)
        eurr = de_cr["extract_using_regex"]
        ex_eurr = {'results': [
            {'origin': {'score': 1.0, 'segment': 'readability_relaxed', 'method': 'extract_using_regex'},
             'context': {'start': 58, 'end': 75, 'input': 'text', 'text': u' 27 \n \n \n My name is Helena height 16'},
             'value': u'Helena'}]}

        self.assertEqual(eurr, ex_eurr)

        self.assertTrue("content_extraction" in r)
        self.assertTrue("title" in r["content_extraction"])
        self.assertTrue("text" in r["content_extraction"]["title"])
        self.assertTrue("tokens" in r["content_extraction"]["title"])
        self.assertTrue("simple_tokens" in r["content_extraction"]["title"])

    def test_extraction_multiple_input_paths(self):
        women_name_file_path = os.path.join(os.path.dirname(__file__), "resources/female-names.json.gz")
        e_config = {
            "resources": {
                "dictionaries": {
                    "women_name": women_name_file_path
                }
            },
            "data_extraction": [
                {
                    "input_path": ["*.*.text.`parent`", "*.inferlink_extractions.*.text.`parent`"],
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

        ex_eud = {'results': [
            {'origin': {'score': 1.0, 'segment': 'readability_strict', 'method': 'extract_using_dictionary'},
             'context': {'end': 11, 'tokens_left': [u'27', u'\n\n\n', u'my', u'name', u'is'],
                         'text': u'27 \n\n\n my name is helena height 160cms weight 55 kilos', 'start': 10,
                         'input': 'tokens', 'tokens_right': [u'height', u'160cms', u'weight', u'55', u'kilos']},
             'value': u'helena'},
            {'origin': {'score': 1.0, 'segment': 'readability_strict', 'method': 'extract_using_dictionary'},
             'context': {'end': 137, 'tokens_left': [u'\n\n', u'hey', u'i', u"'", u'm'],
                         'text': u"\n\n hey i ' m luna 3234522013 let ' s explore", 'start': 136, 'input': 'tokens',
                         'tokens_right': [u'3234522013', u'let', u"'", u's', u'explore']}, 'value': u'luna'}]}
        self.assertEqual(eud, ex_eud)

        self.assertTrue("extract_using_regex" in de_cs)
        eur = de_cs["extract_using_regex"]

        ex_eur = {'results': [
            {'origin': {'score': 1.0, 'segment': 'readability_strict', 'method': 'extract_using_regex'},
             'context': {'start': 56, 'end': 73, 'input': 'text', 'text': u' 27 \n \n \n My name is Helena height 16'},
             'value': u'Helena'}]}

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

        ex_eudr = {'results': [
            {'origin': {'score': 1.0, 'segment': 'readability_relaxed', 'method': 'extract_using_dictionary'},
             'context': {'end': 11, 'tokens_left': [u'27', u'\n\n\n', u'my', u'name', u'is'],
                         'text': u'27 \n\n\n my name is helena height 160cms weight 55 kilos', 'start': 10,
                         'input': 'tokens', 'tokens_right': [u'height', u'160cms', u'weight', u'55', u'kilos']},
             'value': u'helena'},
            {'origin': {'score': 1.0, 'segment': 'readability_relaxed', 'method': 'extract_using_dictionary'},
             'context': {'end': 137, 'tokens_left': [u'\n\n', u'hey', u'i', u"'", u'm'],
                         'text': u"\n\n hey i ' m luna 3234522013 let ' s explore", 'start': 136, 'input': 'tokens',
                         'tokens_right': [u'3234522013', u'let', u"'", u's', u'explore']}, 'value': u'luna'}]}
        self.assertEqual(eudr, ex_eudr)

        self.assertTrue("extract_using_regex" in de_cr)
        eurr = de_cr["extract_using_regex"]

        ex_eurr = {'results': [
            {'origin': {'score': 1.0, 'segment': 'readability_relaxed', 'method': 'extract_using_regex'},
             'context': {'start': 58, 'end': 75, 'input': 'text', 'text': u' 27 \n \n \n My name is Helena height 16'},
             'value': u'Helena'}]}

        self.assertEqual(eurr, ex_eurr)

        self.assertTrue("content_extraction" in r)
        self.assertTrue("title" in r["content_extraction"])
        self.assertTrue("text" in r["content_extraction"]["title"])
        self.assertTrue("tokens" in r["content_extraction"]["title"])
        self.assertTrue("simple_tokens" in r["content_extraction"]["title"])

        self.assertTrue("inferlink_extractions" in r["content_extraction"])
        ie_ex = r["content_extraction"]["inferlink_extractions"]

        self.assertTrue("inferlink_location" in ie_ex)
        self.assertTrue("tokens" in ie_ex["inferlink_location"])
        self.assertTrue("simple_tokens" in ie_ex["inferlink_location"])
        self.assertFalse("data_extraction" in ie_ex["inferlink_location"])

        self.assertTrue("inferlink_age" in ie_ex)
        self.assertTrue("tokens" in ie_ex["inferlink_age"])
        self.assertTrue("simple_tokens" in ie_ex["inferlink_age"])
        self.assertFalse("data_extraction" in ie_ex["inferlink_age"])

        self.assertTrue("inferlink_phone" in ie_ex)
        self.assertTrue("tokens" in ie_ex["inferlink_phone"])
        self.assertTrue("simple_tokens" in ie_ex["inferlink_phone"])
        self.assertFalse("data_extraction" in ie_ex["inferlink_phone"])

        self.assertTrue("inferlink_posting-date" in ie_ex)
        self.assertTrue("tokens" in ie_ex["inferlink_posting-date"])
        self.assertTrue("simple_tokens" in ie_ex["inferlink_posting-date"])
        self.assertFalse("data_extraction" in ie_ex["inferlink_posting-date"])

        self.assertTrue("inferlink_description" in ie_ex)
        self.assertTrue("tokens" in ie_ex["inferlink_description"])
        self.assertTrue("simple_tokens" in ie_ex["inferlink_description"])
        self.assertTrue("data_extraction" in ie_ex["inferlink_description"])

        self.assertTrue("name" in ie_ex["inferlink_description"]["data_extraction"])
        self.assertTrue("extract_using_dictionary" in ie_ex["inferlink_description"]["data_extraction"]["name"])
        ie_desc_ex = ie_ex["inferlink_description"]["data_extraction"]["name"]["extract_using_dictionary"]

        ie_desc_name = {'results': [
            {'origin': {'score': 1.0, 'segment': 'inferlink_description', 'method': 'extract_using_dictionary'},
             'context': {'end': 5, 'tokens_left': [u'hey', u'i', u"'", u'm'],
                         'text': u"hey i ' m luna 3234522013 let ' s explore", 'start': 4, 'input': 'tokens',
                         'tokens_right': [u'3234522013', u'let', u"'", u's', u'explore']}, 'value': u'luna'}]}
        self.assertEqual(ie_desc_ex, ie_desc_name)

        self.assertFalse("extract_using_regex" in ie_ex["inferlink_description"]["data_extraction"]["name"])


if __name__ == '__main__':
    unittest.main()
