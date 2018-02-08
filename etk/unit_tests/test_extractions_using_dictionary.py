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
            "document_id": "doc_id",
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
        self.assertTrue("knowledge_graph" in r)
        self.assertTrue("name" in r["knowledge_graph"])
        self.assertTrue(len(r["knowledge_graph"]["name"]) == 2)
        extraction = r["knowledge_graph"]["name"]
        ex = [
            {
                "confidence": 1,
                "provenance": [
                    {
                        "source": {
                            "segment": "content_strict",
                            "context": {
                                "start": 10,
                                "end": 11,
                                "input": "tokens",
                                "text": "27 \n my name is <etk 'attribute' = 'name'>helena</etk> height 160cms weight 55 kilos "
                            },
                            "document_id": "1A4A5FF5BD066309C72C8EEE6F7BCCCFD21B83245AFCDADDF014455BCF990A21"
                        },
                        "confidence": {
                            "extraction": 1.0
                        },
                        "method": "extract_using_dictionary",
                        "extracted_value": "helena"
                    }
                ],
                "key": "helena",
                "value": "helena"
            },
            {
                "confidence": 1,
                "provenance": [
                    {
                        "source": {
                            "segment": "content_strict",
                            "context": {
                                "start": 136,
                                "end": 137,
                                "input": "tokens",
                                "text": "\n hey i ' m <etk 'attribute' = 'name'>luna</etk> 3234522013 let ' s explore "
                            },
                            "document_id": "1A4A5FF5BD066309C72C8EEE6F7BCCCFD21B83245AFCDADDF014455BCF990A21"
                        },
                        "confidence": {
                            "extraction": 1.0
                        },
                        "method": "extract_using_dictionary",
                        "extracted_value": "luna"
                    }
                ],
                "key": "luna",
                "value": "luna"
            }
        ]
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
            "document_id": "doc_id",
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
        self.assertTrue("knowledge_graph" in r)
        extraction = r["knowledge_graph"]["name"]
        expected_extraction = [
            {
                "confidence": 1,
                "provenance": [
                    {
                        "source": {
                            "segment": "content_strict",
                            "context": {
                                "start": 3,
                                "end": 4,
                                "input": "tokens",
                                "text": "My name is <etk 'attribute' = 'name'>Margie</etk> and this is a test "
                            },
                            "document_id": "id"
                        },
                        "confidence": {
                            "extraction": 1.0
                        },
                        "method": "extract_using_dictionary",
                        "extracted_value": "Margie"
                    }
                ],
                "key": "margie",
                "value": "Margie"
            }
        ]
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
            "document_id": "doc_id",
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

    def test_decode_value_dictionary(self):
        women_name_file_path = os.path.join(os.path.dirname(__file__), "resources/case_sensitive_female_name.json")
        name_decoding_dict_path = os.path.join(os.path.dirname(__file__), "resources/name_decode.json")
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
                },

                "decoding_dictionary": {"name": name_decoding_dict_path}

            },
            "document_id": "doc_id",
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
                                        ],
                                        "post_filter_s": "decode_value"
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
        self.assertEqual(r['knowledge_graph']['name'][0]['value'], 'Not Margie')
        # print json.dumps(r['knowledge_graph'], indent=2)


if __name__ == '__main__':
    unittest.main()
