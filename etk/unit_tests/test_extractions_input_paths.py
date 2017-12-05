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


    def test_invalid_json_path(self):
        doc = {
            "url": "http:www.hitman.org",
            "doc_id": "19B0EAB211CD1D3C63063FAB0B2937043EA1F07B5341014A80E7473BA7318D9E",
            "actors": {
                "name": "agent 47",
                "affiliation": "International Contract Agency"
            }
        }

        e_config = {
            "document_id": "doc_id",
            "data_extraction": [
                {
                    "input_path": [
                        "actors["
                    ],
                    "fields": {
                        "actors": {
                            "extractors": {
                                "create_kg_node_extractor": {
                                    "config": {
                                        "segment_name": "actor_information"
                                    }
                                }
                            }
                        }
                    }
                }
            ]
        }
        c = Core(extraction_config=e_config)

        with self.assertRaises(Exception):
            r = c.process(doc)


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
        r = c.process(self.doc)

        self.assertTrue("content_extraction" in r)
        self.assertTrue("content_strict" in r["content_extraction"])
        self.assertTrue("text" in r["content_extraction"]["content_strict"])
        self.assertTrue("simple_tokens_original_case" in r["content_extraction"]["content_strict"])
        self.assertTrue("simple_tokens" in r["content_extraction"]["content_strict"])
        self.assertTrue("content_relaxed" in r["content_extraction"])
        self.assertTrue("text" in r["content_extraction"]["content_relaxed"])
        self.assertTrue("simple_tokens_original_case" in r["content_extraction"]["content_relaxed"])
        self.assertTrue("simple_tokens" in r["content_extraction"]["content_relaxed"])
        self.assertTrue("title" in r["content_extraction"])
        self.assertTrue("text" in r["content_extraction"]["title"])
        self.assertTrue("simple_tokens_original_case" in r["content_extraction"]["title"])
        self.assertTrue("simple_tokens" in r["content_extraction"]["title"])

        self.assertTrue("knowledge_graph" in r)
        self.assertTrue("name" in r["knowledge_graph"])
        expected = [
            {
                "confidence": 1,
                "provenance": [
                    {
                        "source": {
                            "segment": "content_relaxed",
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
                    },
                    {
                        "source": {
                            "segment": "content_relaxed",
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
                    },
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
                    },
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
                "value": "helena"
            },
            {
                "confidence": 1,
                "provenance": [
                    {
                        "source": {
                            "segment": "content_relaxed",
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
                    },
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
        ]
        results = r["knowledge_graph"]["name"]
        self.assertTrue(expected, results)

    def test_extraction_multiple_input_paths(self):
        women_name_file_path = os.path.join(os.path.dirname(__file__), "resources/female-names.json.gz")
        e_config = {"document_id": "doc_id",
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
        self.assertTrue("simple_tokens_original_case" in r["content_extraction"]["content_strict"])
        self.assertTrue("simple_tokens" in r["content_extraction"]["content_strict"])

        self.assertTrue("content_relaxed" in r["content_extraction"])
        self.assertTrue("text" in r["content_extraction"]["content_relaxed"])
        self.assertTrue("simple_tokens_original_case" in r["content_extraction"]["content_relaxed"])
        self.assertTrue("simple_tokens" in r["content_extraction"]["content_relaxed"])

        self.assertTrue("title" in r["content_extraction"])
        self.assertTrue("text" in r["content_extraction"]["title"])
        self.assertTrue("simple_tokens_original_case" in r["content_extraction"]["title"])
        self.assertTrue("simple_tokens" in r["content_extraction"]["title"])

        self.assertTrue("inferlink_extractions" in r["content_extraction"])
        ie_ex = r["content_extraction"]["inferlink_extractions"]

        self.assertTrue("inferlink_location" in ie_ex)
        self.assertTrue("simple_tokens_original_case" in ie_ex["inferlink_location"])
        self.assertTrue("simple_tokens" in ie_ex["inferlink_location"])

        self.assertTrue("inferlink_age" in ie_ex)
        self.assertTrue("simple_tokens_original_case" in ie_ex["inferlink_age"])
        self.assertTrue("simple_tokens" in ie_ex["inferlink_age"])

        self.assertTrue("inferlink_phone" in ie_ex)
        self.assertTrue("simple_tokens_original_case" in ie_ex["inferlink_phone"])
        self.assertTrue("simple_tokens" in ie_ex["inferlink_phone"])

        self.assertTrue("inferlink_posting-date" in ie_ex)
        self.assertTrue("simple_tokens_original_case" in ie_ex["inferlink_posting-date"])
        self.assertTrue("simple_tokens" in ie_ex["inferlink_posting-date"])

        self.assertTrue("inferlink_description" in ie_ex)
        self.assertTrue("simple_tokens_original_case" in ie_ex["inferlink_description"])
        self.assertTrue("simple_tokens" in ie_ex["inferlink_description"])

        results = r["knowledge_graph"]["name"]

        expected = [
            {
                "confidence": 1,
                "provenance": [
                    {
                        "source": {
                            "segment": "content_relaxed",
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
                    },
                    {
                        "source": {
                            "segment": "content_relaxed",
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
                    },
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
                    },
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
                "value": "helena"
            },
            {
                "confidence": 1,
                "provenance": [
                    {
                        "source": {
                            "segment": "content_relaxed",
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
                    },
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
                    },
                    {
                        "source": {
                            "segment": "inferlink_description",
                            "context": {
                                "start": 4,
                                "end": 5,
                                "input": "tokens",
                                "text": "hey i ' m <etk 'attribute' = 'name'>luna</etk> 3234522013 let ' s explore "
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
        self.assertTrue(expected, results)


if __name__ == '__main__':
    unittest.main()
