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
        self.assertTrue('parent_doc_id' in nested_doc)
        self.assertEqual(nested_doc['parent_doc_id'],
                         '19B0EAB211CD1D3C63063FAB0B2937043EA1F07B5341014A80E7473BA7318D9E')
        self.assertTrue('@timestamp_created' in nested_doc)
        self.assertTrue('url' in nested_doc)

        self.assertEqual(r['knowledge_graph']['actors'][0]['provenance'][0]['qualifiers']['timestamp_created'],
                         nested_doc['@timestamp_created'])


if __name__ == '__main__':
    unittest.main()
