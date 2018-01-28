# -*- coding: utf-8 -*-
import unittest
import sys, os

sys.path.append('../../')
from etk.core import Core
import json
import codecs


class TestExtractionsInputPaths(unittest.TestCase):
    def test_extraction_input_path(self):
        afield_decoding_dict_path = os.path.join(os.path.dirname(__file__), "resources/afield_decoding_dict.json")
        doc = {
            "doc_id": "1",
            'metadata': {
                'eval': 'avalue'
            }
        }
        e_config = {
            "document_id": "doc_id",
            "extraction_policy": "replace",
            "error_handling": "raise_error",
            "resources": {
                "decoding_dictionary": {"afield": afield_decoding_dict_path}
            },
            "content_extraction": {
                "json_content": [
                    {
                        "input_path": "metadata.eval",
                        "segment_name": "measurement__val"
                    }
                ]
            },
            "data_extraction": [
                {
                    "input_path": "content_extraction.measurement__val[*]",
                    "fields": {
                        "afield": {
                            "extractors": {
                                "extract_as_is": {
                                    "config": {
                                        "post_filter": "decode_value"
                                    }
                                }
                            }
                        }
                    }
                }
            ]
        }
        c = Core(extraction_config=e_config)
        r = c.process(doc, create_knowledge_graph=True)
        self.assertEqual(r["knowledge_graph"]["afield"][0]["value"], "xvalue")

if __name__ == '__main__':
    unittest.main()
