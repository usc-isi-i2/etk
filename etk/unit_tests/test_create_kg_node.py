# -*- coding: utf-8 -*-
import unittest
import sys, os
import json

sys.path.append('../../')
from etk.core import Core


class TestCreateKGExtractor(unittest.TestCase):
    def test_create_kg_node(self):
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
                        "actors"
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
        r = c.process(doc)
        self.assertTrue('knowledge_graph' in doc)
        self.assertTrue('actors' in doc['knowledge_graph'])
        self.assertTrue(len(doc['knowledge_graph']['actors']) == 1)
        self.assertTrue('nested_docs' in r)
        self.assertTrue(len(r['nested_docs']) == 1)
        nested_doc = r['nested_docs'][0]
        ce_expected = {
            "actor_information": {
                "affiliation": "International Contract Agency",
                "name": "agent 47"
            }
        }

        self.assertEqual(nested_doc['content_extraction'], ce_expected)
        # self.assertTrue('parent_doc_id' in nested_doc)
        # self.assertEqual(nested_doc['parent_doc_id'],
        #                  '19B0EAB211CD1D3C63063FAB0B2937043EA1F07B5341014A80E7473BA7318D9E')
        self.assertTrue('created_by' in nested_doc)
        self.assertTrue('@timestamp_created' in nested_doc)
        self.assertTrue('url' in nested_doc)

        self.assertEqual(r['knowledge_graph']['actors'][0]['provenance'][0]['qualifiers']['timestamp_created'],
                         nested_doc['@timestamp_created'])

    def test_create_kg_multiple_node(self):
        doc = {
            "url": "http:www.hitman.org",
            "doc_id": "19B0EAB211CD1D3C63063FAB0B2937043EA1F07B5341014A80E7473BA7318D9E",
            "actors": [{
                "name": "agent 47",
                "affiliation": "International Contract Agency"
            },
                {
                    "name": "dr who",
                    "affiliation": "gallifrey"
                }]
        }

        e_config = {
            "document_id": "doc_id",
            "data_extraction": [
                {
                    "input_path": [
                        "actors"
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
        r = c.process(doc)
        self.assertTrue('knowledge_graph' in doc)
        self.assertTrue('actors' in doc['knowledge_graph'])
        self.assertTrue(len(doc['knowledge_graph']['actors']) == 2)
        self.assertTrue('nested_docs' in r)
        self.assertTrue(len(r['nested_docs']) == 2)
        nested_docs = r['nested_docs']
        ce_expected_1 = {
            "actor_information": {
                "affiliation": "International Contract Agency",
                "name": "agent 47"
            }
        }

        ce_expected_2 = {
            "actor_information": {
                "affiliation": "gallifrey",
                "name": "dr who"
            }
        }

        nested_doc_1 = nested_docs[0]
        nested_doc_2 = nested_docs[1]
        self.assertEqual(nested_doc_1['content_extraction'], ce_expected_1)
        # self.assertTrue('parent_doc_id' in nested_doc_1)
        # self.assertEqual(nested_doc_1['parent_doc_id'],
        #                  '19B0EAB211CD1D3C63063FAB0B2937043EA1F07B5341014A80E7473BA7318D9E')
        self.assertTrue('created_by' in nested_doc)
        self.assertTrue('@timestamp_created' in nested_doc_1)
        self.assertTrue('url' in nested_doc_1)

        self.assertEqual(r['knowledge_graph']['actors'][0]['provenance'][0]['qualifiers']['timestamp_created'],
                         nested_doc_1['@timestamp_created'])

        self.assertEqual(nested_doc_2['content_extraction'], ce_expected_2)
        # self.assertTrue('parent_doc_id' in nested_doc_2)
        # self.assertEqual(nested_doc_2['parent_doc_id'],
        #                  '19B0EAB211CD1D3C63063FAB0B2937043EA1F07B5341014A80E7473BA7318D9E')
        self.assertTrue('@timestamp_created' in nested_doc_2)
        self.assertTrue('url' in nested_doc_2)

        self.assertEqual(r['knowledge_graph']['actors'][1]['provenance'][0]['qualifiers']['timestamp_created'],
                         nested_doc_2['@timestamp_created'])

    def test_create_kg_node_text(self):
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
                        "actors.name"
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
        r = c.process(doc)
        self.assertTrue('knowledge_graph' in doc)
        self.assertTrue('actors' in doc['knowledge_graph'])
        self.assertTrue(len(doc['knowledge_graph']['actors']) == 1)
        self.assertTrue('nested_docs' in r)
        self.assertTrue(len(r['nested_docs']) == 1)
        nested_doc = r['nested_docs'][0]
        ce_expected = {
        "actor_information": "agent 47"
      }

        self.assertEqual(nested_doc['content_extraction'], ce_expected)
        # self.assertTrue('parent_doc_id' in nested_doc)
        # self.assertEqual(nested_doc['parent_doc_id'],
        #                  '19B0EAB211CD1D3C63063FAB0B2937043EA1F07B5341014A80E7473BA7318D9E')
        self.assertTrue('created_by' in nested_doc)
        self.assertTrue('@timestamp_created' in nested_doc)
        self.assertTrue('url' in nested_doc)

        self.assertEqual(r['knowledge_graph']['actors'][0]['provenance'][0]['qualifiers']['timestamp_created'],
                         nested_doc['@timestamp_created'])


if __name__ == '__main__':
    unittest.main()
