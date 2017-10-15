# -*- coding: utf-8 -*-
import unittest
import sys, os

sys.path.append('../../')
from etk.core import Core
import json
import codecs


class TestExtractionsFilterResults(unittest.TestCase):

    def test_filter_results(self):
        doc = {
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
                ]
            }
        }
        e_config = {
            "document_id":"doc_id",
            "resources": {
                "stop_word_dictionaries": {
                    "name": "resources/stop_word_names.json"
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
        c = Core(extraction_config=e_config)
        r = c.process(doc)
        self.assertTrue('knowledge_graph' in doc)
        self.assertTrue('name' in doc['knowledge_graph'])
        self.assertTrue(len(doc['knowledge_graph']['name']) == 1)
        self.assertTrue(doc['knowledge_graph']['name'][0]['confidence'] == 0.3)


if __name__ == '__main__':
    unittest.main()
