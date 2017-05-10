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
              "name": [
                {
                  "confidence": 1000,
                  "provenance": [
                    {
                      "source": {
                        "segment": "content_relaxed",
                        "context": {
                          "end": 11,
                          "tokens_left": [
                            "27",
                            "\n\n\n",
                            "my",
                            "name",
                            "is"
                          ],
                          "text": "27 \n\n\n my name is <etk 'attribute' = 'name'>helena</etk> height 160cms weight 55 kilos ",
                          "start": 10,
                          "input": "tokens",
                          "tokens_right": [
                            "height",
                            "160cms",
                            "weight",
                            "55",
                            "kilos"
                          ]
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
                          "start": 58,
                          "end": 75,
                          "input": "text",
                          "text": " 27 \n \n \n  <etk 'attribute' = 'name'>My name is Helena</etk>  height 16"
                        },
                        "document_id": "1A4A5FF5BD066309C72C8EEE6F7BCCCFD21B83245AFCDADDF014455BCF990A21"
                      },
                      "confidence": {
                        "extraction": 1.0
                      },
                      "method": "extract_using_regex",
                      "extracted_value": "helena"
                    },
                    {
                      "source": {
                        "segment": "content_strict",
                        "context": {
                          "end": 11,
                          "tokens_left": [
                            "27",
                            "\n\n\n",
                            "my",
                            "name",
                            "is"
                          ],
                          "text": "27 \n\n\n my name is <etk 'attribute' = 'name'>helena</etk> height 160cms weight 55 kilos ",
                          "start": 10,
                          "input": "tokens",
                          "tokens_right": [
                            "height",
                            "160cms",
                            "weight",
                            "55",
                            "kilos"
                          ]
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
                          "start": 56,
                          "end": 73,
                          "input": "text",
                          "text": " 27 \n \n \n  <etk 'attribute' = 'name'>My name is Helena</etk>  height 16"
                        },
                        "document_id": "1A4A5FF5BD066309C72C8EEE6F7BCCCFD21B83245AFCDADDF014455BCF990A21"
                      },
                      "confidence": {
                        "extraction": 1.0
                      },
                      "method": "extract_using_regex",
                      "extracted_value": "helena"
                    }
                  ],
                  "value": "helena",
                  "key": "helena"
                },
                {
                  "confidence": 1000,
                  "provenance": [
                    {
                      "source": {
                        "segment": "content_relaxed",
                        "context": {
                          "end": 137,
                          "tokens_left": [
                            "\n\n",
                            "hey",
                            "i",
                            "'",
                            "m"
                          ],
                          "text": "\n\n hey i ' m <etk 'attribute' = 'name'>luna</etk> 3234522013 let ' s explore ",
                          "start": 136,
                          "input": "tokens",
                          "tokens_right": [
                            "3234522013",
                            "let",
                            "'",
                            "s",
                            "explore"
                          ]
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
                          "end": 137,
                          "tokens_left": [
                            "\n\n",
                            "hey",
                            "i",
                            "'",
                            "m"
                          ],
                          "text": "\n\n hey i ' m <etk 'attribute' = 'name'>luna</etk> 3234522013 let ' s explore ",
                          "start": 136,
                          "input": "tokens",
                          "tokens_right": [
                            "3234522013",
                            "let",
                            "'",
                            "s",
                            "explore"
                          ]
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
                          "end": 10,
                          "tokens_left": [
                            "2013",
                            "escort",
                            "alert",
                            "!",
                            "-"
                          ],
                          "text": "2013 escort alert ! - <etk 'attribute' = 'name'>luna</etk> the hot playmate ( 323 ",
                          "start": 9,
                          "input": "tokens",
                          "tokens_right": [
                            "the",
                            "hot",
                            "playmate",
                            "(",
                            "323"
                          ]
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
                  "value": "luna",
                  "key": "luna"
                }
              ]
            }
        self.assertEqual(kg, expected_kg)


if __name__ == '__main__':
    unittest.main()
