# -*- coding: utf-8 -*-
import unittest
import sys, os

sys.path.append('../../')
from etk.core import Core
import json
import codecs


class TestExtractionFromLandmark(unittest.TestCase):
    def setUp(self):
        file_path = os.path.join(os.path.dirname(__file__), "ground_truth/1_content_extracted.jl")
        self.doc = json.load(codecs.open(file_path))

    def test_extraction_from_landmark_fields(self):
        e_config = {
            "document_id": "doc_id",
            "data_extraction": [
                {
                    "input_path": [
                        "*.inferlink_extractions.*.text.`parent`"
                    ],
                    "fields": {
                        "location": {
                            "extractors": {
                                "extract_from_landmark": {
                                    "config": {
                                        "fields": [
                                            "inferlink_location"
                                        ]
                                    }
                                }
                            }
                        },
                        "name": {
                            "extractors": {
                                "extract_from_landmark": {
                                    "config": {
                                        "fields": [
                                            "inferlink_name"
                                        ],
                                        "post_filter": [
                                            "x.replace('\t', '')"
                                        ]
                                    },
                                    "extraction_policy": "replace"
                                }
                            }
                        },
                        "age": {
                            "extractors": {
                                "extract_from_landmark": {
                                    "config": {
                                        "fields": [
                                            "inferlink_age"
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

        inferlink_extractions = r["content_extraction"]["inferlink_extractions"]

        self.assertTrue("inferlink_location" in inferlink_extractions)
        self.assertTrue("knowledge_graph" in r)
        ex_location = r["knowledge_graph"]["location"]
        ifl_location = [
            {
                "confidence": 1,
                "provenance": [
                    {
                        "source": {
                            "segment": "html",
                            "document_id": "1A4A5FF5BD066309C72C8EEE6F7BCCCFD21B83245AFCDADDF014455BCF990A21"
                        },
                        "confidence": {
                            "extraction": 1.0
                        },
                        "method": "inferlink",
                        "extracted_value": "Los Angeles, California"
                    }
                ],
                "key": "los angeles, california",
                "value": "Los Angeles, California"
            }
        ]
        self.assertEqual(ex_location, ifl_location)

        self.assertTrue("inferlink_age" in inferlink_extractions)

        ex_age = r["knowledge_graph"]["age"]

        ifl_age = [
            {
                "confidence": 1,
                "provenance": [
                    {
                        "source": {
                            "segment": "html",
                            "document_id": "1A4A5FF5BD066309C72C8EEE6F7BCCCFD21B83245AFCDADDF014455BCF990A21"
                        },
                        "confidence": {
                            "extraction": 1.0
                        },
                        "method": "inferlink",
                        "extracted_value": "23"
                    }
                ],
                "key": "23",
                "value": "23"
            }
        ]

        self.assertEqual(ex_age, ifl_age)

        self.assertFalse("inferlink_name" in inferlink_extractions)

    def test_extract_landmark_no_fields(self):
        e_config = {
            "document_id": "doc_id",
            "data_extraction": [
                {
                    "input_path": [
                        "*.inferlink_extractions.*.text.`parent`"
                    ],
                    "fields": {
                        "posting-date": {
                            "extractors": {
                                "extract_from_landmark":
                                    {

                                    }
                            }
                        }
                    }
                }
            ]
        }

        c = Core(extraction_config=e_config)
        r = c.process(self.doc)
        self.assertTrue("knowledge_graph" in r)
        ex_posting_date = r["knowledge_graph"]["posting-date"]

        ifl_posting_date = [
            {
                "confidence": 1,
                "provenance": [
                    {
                        "source": {
                            "segment": "html",
                            "document_id": "1A4A5FF5BD066309C72C8EEE6F7BCCCFD21B83245AFCDADDF014455BCF990A21"
                        },
                        "confidence": {
                            "extraction": 1.0
                        },
                        "method": "inferlink",
                        "extracted_value": "2017-01-02 06:46"
                    }
                ],
                "key": "2017-01-02 06:46",
                "value": "2017-01-02 06:46"
            }
        ]

        self.assertEqual(ex_posting_date, ifl_posting_date)

    def test_extract_landmark_post_filter(self):
        e_config = {
            "document_id": "doc_id",
            "data_extraction": [
                {
                    "input_path": [
                        "*.inferlink_extractions.*.text.`parent`"
                    ],
                    "fields": {
                        "phone": {
                            "extractors": {
                                "extract_from_landmark": {
                                    "config": {
                                        "fields": [
                                            "inferlink_phone"
                                        ],
                                        "post_filter": [
                                            "extract_phone"
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
        self.assertTrue("knowledge_graph" in r)
        ex_phone = r["knowledge_graph"]["phone"]

        expected_phone = [
            {
                "confidence": 1,
                "provenance": [
                    {
                        "source": {
                            "extraction_metadata": {
                                "obfuscation": "False"
                            },
                            "segment": "html",
                            "document_id": "1A4A5FF5BD066309C72C8EEE6F7BCCCFD21B83245AFCDADDF014455BCF990A21"
                        },
                        "confidence": {
                            "extraction": 1.0
                        },
                        "method": "inferlink",
                        "extracted_value": "3234522013"
                    }
                ],
                "key": "3234522013",
                "value": "3234522013"
            }
        ]
        self.assertEqual(ex_phone, expected_phone)


if __name__ == '__main__':
    unittest.main()
