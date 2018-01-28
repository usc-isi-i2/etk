# -*- coding: utf-8 -*-
import unittest
import sys, os

sys.path.append('../../')
from etk.core import Core
import json
import codecs


class TestExtractionsInputPaths(unittest.TestCase):
    def setUp(self):
        file_path = os.path.join(os.path.dirname(__file__), "ground_truth/custom_spacy.jl")
        self.doc = json.load(codecs.open(file_path))

    def test_extraction_input_path(self):
        spacy_rule_file_path = os.path.join(os.path.dirname(__file__), "resources/custom_spacy_rule.json")
        e_config = {
            "document_id": "doc_id",
            "extraction_policy": "replace",
            "error_handling": "raise_error",
            "resources": {
                "spacy_field_rules": {"JUNK": spacy_rule_file_path}
            },
            "content_extraction": {
                "input_path": "raw_content",
                "extractors": {
                    "readability": [
                        {
                            "strict": "yes",
                            "extraction_policy": "keep_existing",
                            "field_name": "content_strict"
                        },
                        {
                            "strict": "no",
                            "extraction_policy": "keep_existing",
                            "field_name": "content_relaxed"
                        }
                    ],
                    "title": {
                        "extraction_policy": "keep_existing"
                    }
                }
            },
            "data_extraction": [
                {
                    "input_path": [
                        "*.content_strict.text.`parent`",
                        "*.content_relaxed.text.`parent`",
                        "*.title.text.`parent`"
                    ],
                    "fields": {
                        "JUNK": {
                            "extractors": {
                                "extract_using_custom_spacy": {
                                    "config": {
                                        "spacy_field_rules": "JUNK"
                                    }
                                }
                            }
                        }
                    }
                }
            ]
        }
        c = Core(extraction_config=e_config)
        r = c.process(self.doc, create_knowledge_graph=True)
        # self.assertTrue('knowledge_graph' in r)
        if 'JUNK' in r['knowledge_graph']:
            custom_spacy_extracted = r['knowledge_graph']['JUNK']
        else:
            custom_spacy_extracted = []
        self.assertTrue(len(custom_spacy_extracted) == 3)
        for s in custom_spacy_extracted:
            self.assertTrue(s['value'] in ['Girl', 'runqi', 'lAdy'])

    def test_date_generic(self):
        doc = {
            "doc_id": "1",
            'metadata': {
                'edate': '1/1990'
            }
        }

        e_config = {
            "document_id": "doc_id",
            "resources": {
                "spacy_field_rules": {
                    "measurement_date_rules": os.path.join(os.path.dirname(__file__),
                                                           "resources/measurement_date_rules.json")
                }
            },
            "content_extraction": {
                "json_content": [
                    {
                        "input_path": "metadata.edate",
                        "segment_name": "measurement__date"
                    }
                ]
            },
            "data_extraction": [{
                "fields": {
                    "event_date": {
                        "extractors": {
                            "extract_using_custom_spacy": {
                                "config": {
                                    "spacy_field_rules": "measurement_date_rules",
                                    "post_filter": "parse_date_generic"
                                }
                            }
                        }
                    }
                },
                "input_path": "content_extraction.measurement__date[*]"
            }]}
        c = Core(extraction_config=e_config)
        r = c.process(doc, create_knowledge_graph=True)
        self.assertEqual(r['knowledge_graph']['event_date'][0]['value'], '1990-01-01T00:00:00')


if __name__ == '__main__':
    unittest.main()
