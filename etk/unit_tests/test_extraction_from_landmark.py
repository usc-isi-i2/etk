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
        self.assertTrue("data_extraction" in inferlink_extractions["inferlink_location"])
        self.assertTrue("location" in inferlink_extractions["inferlink_location"]["data_extraction"])
        self.assertTrue(
            "extract_from_landmark" in inferlink_extractions["inferlink_location"]["data_extraction"]["location"])

        ex_location = inferlink_extractions["inferlink_location"]["data_extraction"]["location"][
            "extract_from_landmark"]

        ifl_location = {'results': [
            {'origin': {'score': 1.0, 'segment': 'html', 'method': 'inferlink'}, 'value': u'Los Angeles, California'}]}
        self.assertEqual(ex_location, ifl_location)

        self.assertTrue("inferlink_age" in inferlink_extractions)
        self.assertTrue("data_extraction" in inferlink_extractions["inferlink_age"])
        self.assertTrue("age" in inferlink_extractions["inferlink_age"]["data_extraction"])
        self.assertTrue(
            "extract_from_landmark" in inferlink_extractions["inferlink_age"]["data_extraction"]["age"])
        ex_age = inferlink_extractions["inferlink_age"]["data_extraction"]["age"]["extract_from_landmark"]

        ifl_age = {
            'results': [{'origin': {'score': 1.0, 'segment': 'html', 'method': 'inferlink'}, 'value': '23'}]}

        self.assertEqual(ex_age, ifl_age)

        self.assertFalse("inferlink_name" in inferlink_extractions)

    def test_extract_landmark_no_fields(self):
        e_config = {
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
        inferlink_extractions = r["content_extraction"]["inferlink_extractions"]
        self.assertTrue("data_extraction" in inferlink_extractions["inferlink_posting-date"])
        self.assertTrue("posting-date" in inferlink_extractions["inferlink_posting-date"]["data_extraction"])
        self.assertTrue("extract_from_landmark" in inferlink_extractions["inferlink_posting-date"]["data_extraction"][
            "posting-date"])

        ex_posting_date = inferlink_extractions["inferlink_posting-date"]["data_extraction"]["posting-date"][
            "extract_from_landmark"]

        ifl_posting_date = {'results': [
            {'origin': {'score': 1.0, 'segment': 'html', 'method': 'inferlink'}, 'value': u'2017-01-02 06:46'}]}

        self.assertEqual(ex_posting_date, ifl_posting_date)

    def test_extract_landmark_post_filter(self):
        e_config = {
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
        inferlink_extractions = r["content_extraction"]["inferlink_extractions"]

        self.assertTrue("data_extraction" in inferlink_extractions["inferlink_phone"])
        self.assertTrue("phone" in inferlink_extractions["inferlink_phone"]["data_extraction"])
        self.assertTrue("extract_from_landmark" in inferlink_extractions["inferlink_phone"]["data_extraction"]["phone"])
        ex_phone = inferlink_extractions["inferlink_phone"]["data_extraction"]["phone"]["extract_from_landmark"]

        expected_phone = {'results': [
            {'origin': {'score': 1.0, 'segment': 'html', 'method': 'inferlink'}, 'obfuscation': 'False',
             'value': '3234522013'}]}
        self.assertEqual(ex_phone, expected_phone)


if __name__ == '__main__':
    unittest.main()
