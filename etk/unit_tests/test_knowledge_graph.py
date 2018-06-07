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
        e_config = {"document_id": "doc_id",
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
        r = c.process(self.doc, create_knowledge_graph=True)
        self.assertTrue('knowledge_graph' in r)
        kg = r['knowledge_graph']
        expected_kg = {
            "title": [
                {
                    "confidence": 1,
                    "provenance": [
                        {
                            "source": {
                                "segment": "html",
                                "document_id": "1A4A5FF5BD066309C72C8EEE6F7BCCCFD21B83245AFCDADDF014455BCF990A21"
                            },
                            "method": "rearrange_title"
                        }
                    ],
                    "key": "title",
                    "value": "323-452-2013 ESCORT ALERT! - Luna The Hot Playmate (323) 452-2013 - 23"
                }
            ],
            "name": [
                {
                    "confidence": 1.0,
                    "provenance": [
                        {
                            "source": {
                                "segment": "content_relaxed",
                                "context": {
                                    "start": 7,
                                    "end": 8,
                                    "input": "tokens",
                                    "text": "chrissy391 27 my name is <etk 'attribute' = 'name'>helena</etk> height 160cms weight 55 kilos "
                                },
                                "document_id": "1A4A5FF5BD066309C72C8EEE6F7BCCCFD21B83245AFCDADDF014455BCF990A21"
                            },
                            "confidence": {
                                "extraction": 1.0
                            },
                            "method": "extract_using_dictionary",
                            "extracted_value": "helena"
                        },
                        {
                            "source": {
                                "segment": "content_relaxed",
                                "context": {
                                    "start": 68,
                                    "end": 85,
                                    "input": "text",
                                    "text": "br/><br/>  <etk 'attribute' = 'name'>My name is Helena</etk>  height 16"
                                },
                                "document_id": "1A4A5FF5BD066309C72C8EEE6F7BCCCFD21B83245AFCDADDF014455BCF990A21"
                            },
                            "confidence": {
                                "extraction": 1.0
                            },
                            "method": "extract_using_regex",
                            "extracted_value": "Helena"
                        },
                        {
                            "source": {
                                "segment": "content_strict",
                                "context": {
                                    "start": 7,
                                    "end": 8,
                                    "input": "tokens",
                                    "text": "chrissy391 27 my name is <etk 'attribute' = 'name'>helena</etk> height 160cms weight 55 kilos "
                                },
                                "document_id": "1A4A5FF5BD066309C72C8EEE6F7BCCCFD21B83245AFCDADDF014455BCF990A21"
                            },
                            "confidence": {
                                "extraction": 1.0
                            },
                            "method": "extract_using_dictionary",
                            "extracted_value": "helena"
                        },
                        {
                            "source": {
                                "segment": "content_strict",
                                "context": {
                                    "start": 68,
                                    "end": 85,
                                    "input": "text",
                                    "text": "br/><br/>  <etk 'attribute' = 'name'>My name is Helena</etk>  height 16"
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
                    "value": "helena"
                },
                {
                    "confidence": 1.0,
                    "provenance": [
                        {
                            "source": {
                                "segment": "content_relaxed",
                                "context": {
                                    "start": 111,
                                    "end": 112,
                                    "input": "tokens",
                                    "text": "girls hey i ' m <etk 'attribute' = 'name'>luna</etk> 3234522013 let ' s explore "
                                },
                                "document_id": "1A4A5FF5BD066309C72C8EEE6F7BCCCFD21B83245AFCDADDF014455BCF990A21"
                            },
                            "confidence": {
                                "extraction": 1.0
                            },
                            "method": "extract_using_dictionary",
                            "extracted_value": "luna"
                        },
                        {
                            "source": {
                                "segment": "content_strict",
                                "context": {
                                    "start": 111,
                                    "end": 112,
                                    "input": "tokens",
                                    "text": "girls hey i ' m <etk 'attribute' = 'name'>luna</etk> 3234522013 let ' s explore "
                                },
                                "document_id": "1A4A5FF5BD066309C72C8EEE6F7BCCCFD21B83245AFCDADDF014455BCF990A21"
                            },
                            "confidence": {
                                "extraction": 1.0
                            },
                            "method": "extract_using_dictionary",
                            "extracted_value": "luna"
                        },
                        {
                            "source": {
                                "segment": "title",
                                "context": {
                                    "start": 9,
                                    "end": 10,
                                    "input": "tokens",
                                    "text": "2013 escort alert ! - <etk 'attribute' = 'name'>luna</etk> the hot playmate ( 323 "
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
            ],
            "description": [
                {
                    "confidence": 1,
                    "provenance": [
                        {
                            "source": {
                                "segment": "inferlink",
                                "document_id": "1A4A5FF5BD066309C72C8EEE6F7BCCCFD21B83245AFCDADDF014455BCF990A21"
                            },
                            "method": "rearrange_description"
                        }
                    ],
                    "key": "description",
                    "value": "Hey I'm luna 3234522013 Let's explore , embrace and indulge in your favorite fantasy % independent. discreet no drama Firm Thighs and Sexy. My Soft skin & Tight Grip is exactly what you deserve Call or text Fetish friendly Fantasy friendly Party friendly 140 Hr SPECIALS 3234522013"
                }
            ]
        }
        for key in kg.keys():
            self.assertTrue(key in expected_kg)
            if key != 'title' and key != 'description':
                self.assertEqual(kg[key], expected_kg[key])


if __name__ == '__main__':
    unittest.main()
