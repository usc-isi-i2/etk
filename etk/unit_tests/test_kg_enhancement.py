# -*- coding: utf-8 -*-
import unittest
import sys, os

sys.path.append('../../')
from etk.core import Core
import json
import codecs


class TestExtractionsFilterResults(unittest.TestCase):
    def setUp(self):
        self.doc = {
            "url": "http://www.testurl.com",
            "doc_id": "19B0EAB211CD1D3C63063FAB0B2937043EA1F07B5341014A80E7473BA7318D9E",
            "knowledge_graph": {
                "name": [
                    {
                        "provenance": [
                            {
                                "extracted_value": "Very",
                                "method": "extract_using_custom_spacy",
                                "confidence": {
                                    "extraction": 1
                                },
                                "source": {
                                    "segment": "content_strict",
                                    "context": {
                                        "rule_id": 1,
                                        "input": "tokens",
                                        "identifier": "name_rule_02",
                                        "start": 18,
                                        "end": 21,
                                        "text": ". \n Well Guess What <etk 'attribute' = 'name'>i am Very</etk> Real \n I DON ' "
                                    },
                                    "document_id": "19B0EAB211CD1D3C63063FAB0B2937043EA1F07B5341014A80E7473BA7318D9E"
                                }
                            }
                        ],
                        "confidence": 1.0,
                        "value": "Very",
                        "key": "very"
                    }
                ],
                "fieldA": [
                    {
                        "provenance": [
                            {
                                "extracted_value": "sachin",
                                "method": "extract_using_custom_spacy",
                                "confidence": {
                                    "extraction": 1
                                },
                                "source": {
                                    "segment": "content_strict",
                                    "context": {
                                        "rule_id": 1,
                                        "input": "tokens",
                                        "identifier": "name_rule_02",
                                        "start": 18,
                                        "end": 21,
                                        "text": ". \n Well Guess What <etk 'attribute' = 'name'>i am sachin</etk> Real \n I DON ' "
                                    },
                                    "document_id": "19B0EAB211CD1D3C63063FAB0B2937043EA1F07B5341014A80E7473BA7318D9E"
                                }
                            }
                        ],
                        "confidence": 1.0,
                        "value": "sachin",
                        "key": "Sachin"
                    }
                ]
            }
        }
        stop_words_path = os.path.join(os.path.dirname(__file__), "resources/stop_word_names.json")
        self.e_config = {
            "document_id": "doc_id",
            "resources": {
                "stop_word_dictionaries": {
                    "name": stop_words_path
                }
            },
            "kg_enhancement": {
                "fields": {
                    "name": {
                        "priority": 0,
                        "extractors": {
                            "filter_results": {
                                "config": {
                                    "stop_word_dictionaries": "name"
                                }
                            }
                        }
                    }
                },
                "input_path": "knowledge_graph.`parent`"
            }}

    def test_filter_results(self):
        c = Core(extraction_config=self.e_config)
        r = c.process(self.doc)
        self.assertTrue('knowledge_graph' in self.doc)
        self.assertTrue('name' in self.doc['knowledge_graph'])
        self.assertTrue(len(self.doc['knowledge_graph']['name']) == 1)
        self.assertTrue(self.doc['knowledge_graph']['name'][0]['confidence'] == 0.3)

    def test_guard_url_pass(self):
        self.e_config['kg_enhancement']['fields']['name']['guard'] = [{
            "field": "url",
            "value": "http://www.testurl.com"
        }]
        c = Core(extraction_config=self.e_config)
        r = c.process(self.doc)

        self.assertTrue('knowledge_graph' in self.doc)
        self.assertTrue('name' in self.doc['knowledge_graph'])
        self.assertTrue(len(self.doc['knowledge_graph']['name']) == 1)
        self.assertTrue(self.doc['knowledge_graph']['name'][0]['confidence'] == 0.3)

    def test_guard_url_fail(self):
        self.e_config['kg_enhancement']['fields']['name']['guard'] = [{
            "field": "url",
            "value": "http://www.testffffffurl.com"
        }]
        c = Core(extraction_config=self.e_config)
        r = c.process(self.doc)

        self.assertTrue('knowledge_graph' in self.doc)
        self.assertTrue('name' in self.doc['knowledge_graph'])
        self.assertTrue(len(self.doc['knowledge_graph']['name']) == 1)
        self.assertTrue(self.doc['knowledge_graph']['name'][0]['confidence'] == 1.0)

    def test_guard_url_regex_pass(self):
        self.e_config['kg_enhancement']['fields']['name']['guard'] = [{
            "field": "url",
            "regex": "test*"
        }]
        c = Core(extraction_config=self.e_config)
        r = c.process(self.doc)

        self.assertTrue('knowledge_graph' in self.doc)
        self.assertTrue('name' in self.doc['knowledge_graph'])
        self.assertTrue(len(self.doc['knowledge_graph']['name']) == 1)
        self.assertTrue(self.doc['knowledge_graph']['name'][0]['confidence'] == 0.3)

    def test_guard_url_regex_fail(self):
        self.e_config['kg_enhancement']['fields']['name']['guard'] = [{
            "field": "url",
            "regex": "^test*"
        }]
        c = Core(extraction_config=self.e_config)
        r = c.process(self.doc)

        self.assertTrue('knowledge_graph' in self.doc)
        self.assertTrue('name' in self.doc['knowledge_graph'])
        self.assertTrue(len(self.doc['knowledge_graph']['name']) == 1)
        self.assertTrue(self.doc['knowledge_graph']['name'][0]['confidence'] == 1.0)

    def test_guard_field_value_pass(self):
        self.e_config['kg_enhancement']['fields']['name']['guard'] = [{
            "field": "fieldA",
            "value": "sachin"
        }]
        c = Core(extraction_config=self.e_config)
        r = c.process(self.doc)

        self.assertTrue('knowledge_graph' in self.doc)
        self.assertTrue('name' in self.doc['knowledge_graph'])
        self.assertTrue(len(self.doc['knowledge_graph']['name']) == 1)
        self.assertTrue(self.doc['knowledge_graph']['name'][0]['confidence'] == 0.3)

    def test_guard_field_value_fail(self):
        self.e_config['kg_enhancement']['fields']['name']['guard'] = [{
            "field": "fieldA",
            "value": "SACHIN"
        }]
        c = Core(extraction_config=self.e_config)
        r = c.process(self.doc)

        self.assertTrue('knowledge_graph' in self.doc)
        self.assertTrue('name' in self.doc['knowledge_graph'])
        self.assertTrue(len(self.doc['knowledge_graph']['name']) == 1)
        self.assertTrue(self.doc['knowledge_graph']['name'][0]['confidence'] == 1.0)

    def test_guard_field_value_match_pass(self):
        self.e_config['kg_enhancement']['fields']['name']['guard'] = [{
            "field": "fieldA",
            "value": "Sachin",
            "match": "key"
        }]
        c = Core(extraction_config=self.e_config)
        r = c.process(self.doc)

        self.assertTrue('knowledge_graph' in self.doc)
        self.assertTrue('name' in self.doc['knowledge_graph'])
        self.assertTrue(len(self.doc['knowledge_graph']['name']) == 1)
        self.assertTrue(self.doc['knowledge_graph']['name'][0]['confidence'] == 0.3)

    def test_guard_field_value_match_fail(self):
        self.e_config['kg_enhancement']['fields']['name']['guard'] = [{
            "field": "fieldA",
            "value": "SAchin",
            "match": "key"
        }]
        c = Core(extraction_config=self.e_config)
        r = c.process(self.doc)

        self.assertTrue('knowledge_graph' in self.doc)
        self.assertTrue('name' in self.doc['knowledge_graph'])
        self.assertTrue(len(self.doc['knowledge_graph']['name']) == 1)
        self.assertTrue(self.doc['knowledge_graph']['name'][0]['confidence'] == 1.0)

    def test_guard_field_stop_value_pass(self):
        self.e_config['kg_enhancement']['fields']['name']['guard'] = [{
            "field": "fieldA",
            "stop_value": "sachin"
        }]
        c = Core(extraction_config=self.e_config)
        r = c.process(self.doc)

        self.assertTrue('knowledge_graph' in self.doc)
        self.assertTrue('name' in self.doc['knowledge_graph'])
        self.assertTrue(len(self.doc['knowledge_graph']['name']) == 1)
        self.assertTrue(self.doc['knowledge_graph']['name'][0]['confidence'] == 1.0)

    def test_guard_field_stop_value_fail(self):
        self.e_config['kg_enhancement']['fields']['name']['guard'] = [{
            "field": "fieldA",
            "stop_value": "SACHIN"
        }]
        c = Core(extraction_config=self.e_config)
        r = c.process(self.doc)

        self.assertTrue('knowledge_graph' in self.doc)
        self.assertTrue('name' in self.doc['knowledge_graph'])
        self.assertTrue(len(self.doc['knowledge_graph']['name']) == 1)
        self.assertTrue(self.doc['knowledge_graph']['name'][0]['confidence'] == 0.3)

    def test_guard_field_regex_pass(self):
        self.e_config['kg_enhancement']['fields']['name']['guard'] = [{
            "field": "fieldA",
            "regex": "ach"
        }]
        c = Core(extraction_config=self.e_config)
        r = c.process(self.doc)

        self.assertTrue('knowledge_graph' in self.doc)
        self.assertTrue('name' in self.doc['knowledge_graph'])
        self.assertTrue(len(self.doc['knowledge_graph']['name']) == 1)
        self.assertTrue(self.doc['knowledge_graph']['name'][0]['confidence'] == 0.3)

    def test_guard_field_regex_fail(self):
        self.e_config['kg_enhancement']['fields']['name']['guard'] = [{
            "field": "fieldA",
            "regex": "^ach"
        }]
        c = Core(extraction_config=self.e_config)
        r = c.process(self.doc)

        self.assertTrue('knowledge_graph' in self.doc)
        self.assertTrue('name' in self.doc['knowledge_graph'])
        self.assertTrue(len(self.doc['knowledge_graph']['name']) == 1)
        self.assertTrue(self.doc['knowledge_graph']['name'][0]['confidence'] == 1.0)

    def test_guard_multi_pass(self):
        self.e_config['kg_enhancement']['fields']['name']['guard'] = [{
            "field": "fieldA",
            "regex": "ach"
        },
            {
                "field": "url",
                "regex": "test"
            }
        ]
        c = Core(extraction_config=self.e_config)
        r = c.process(self.doc)

        self.assertTrue('knowledge_graph' in self.doc)
        self.assertTrue('name' in self.doc['knowledge_graph'])
        self.assertTrue(len(self.doc['knowledge_graph']['name']) == 1)
        self.assertTrue(self.doc['knowledge_graph']['name'][0]['confidence'] == 0.3)

    def test_guard_multi_fail(self):
        self.e_config['kg_enhancement']['fields']['name']['guard'] = [{
            "field": "fieldA",
            "regex": "ach"
        },
            {
                "field": "url",
                "regex": "^test"
            }
        ]
        c = Core(extraction_config=self.e_config)
        r = c.process(self.doc)

        self.assertTrue('knowledge_graph' in self.doc)
        self.assertTrue('name' in self.doc['knowledge_graph'])
        self.assertTrue(len(self.doc['knowledge_graph']['name']) == 1)
        self.assertTrue(self.doc['knowledge_graph']['name'][0]['confidence'] == 1.0)

    def test_add_constants(self):
        e_config = {
            "document_id": "doc_id",
            "kg_enhancement": {
                "fields": {
                    "type": {
                        "priority": 0,
                        "extractors": {
                            "add_constant_kg": {
                                "config": {
                                    "constants": ["Type A", "Type B"]
                                }
                            }
                        }
                    }
                },
                "input_path": "knowledge_graph.`parent`"
            }}

        c = Core(extraction_config=e_config)
        r = c.process(self.doc)
        self.assertTrue('knowledge_graph' in self.doc)
        self.assertTrue('type' in self.doc['knowledge_graph'])
        self.assertTrue(len(self.doc['knowledge_graph']['type']) == 2)
        self.assertTrue(self.doc['knowledge_graph']['type'][2]['value'] in ["Type A", "Type B"])


if __name__ == '__main__':
    unittest.main()
