# -*- coding: utf-8 -*-
import unittest
import sys, os

sys.path.append('../../')
from etk.core import Core
import json
import codecs


class TestDocFilters(unittest.TestCase):
    def test_filter_results(self):
        doc = {
            "url": "http://www.testurl.com",
            "doc_id": "19B0EAB211CD1D3C63063FAB0B2937043EA1F07B5341014A80E7473BA7318D9E"
        }
        e_config = {
            "document_id": "doc_id",
            "filters": {
                "testurl.com": {
                    "field": "url",
                    "action": "keep",
                    "regex": "test*"
                }
            }
        }
        c = Core(extraction_config=e_config)
        doc = c.process_doc_filters(doc)
        self.assertTrue('prefilter_filter_outcome' in doc)
        self.assertTrue(doc['prefilter_filter_outcome'] == 'keep')

    def test_filter_results_incomplete_filter(self):
        doc = {
            "url": "http://www.testurl.com",
            "doc_id": "19B0EAB211CD1D3C63063FAB0B2937043EA1F07B5341014A80E7473BA7318D9E"
        }
        e_config = {
            "document_id": "doc_id",
            "filters": {
                "testurl.com": {
                    "field": "url",
                    "regex": "test*"
                }
            }
        }
        c = Core(extraction_config=e_config)
        doc = c.process_doc_filters(doc)
        self.assertTrue('prefilter_filter_outcome' in doc)
        self.assertTrue(doc['prefilter_filter_outcome'] == 'no_action')

    def test_filter_results_multiple_filters(self):
        doc = {
            "url": "http://www.testurl.com",
            "doc_id": "19B0EAB211CD1D3C63063FAB0B2937043EA1F07B5341014A80E7473BA7318D9E"
        }
        e_config = {
            "document_id": "doc_id",
            "filters": {
                "testurl.com": [{
                    "field": "url",
                    "action": "keep",
                    "regex": "testt"
                },{
                    "field": "url",
                    "action": "discard",
                    "regex": "test*"
                }]
            }
        }
        c = Core(extraction_config=e_config)
        doc = c.process_doc_filters(doc)
        self.assertTrue('prefilter_filter_outcome' in doc)
        self.assertTrue(doc['prefilter_filter_outcome'] == 'discard')


if __name__ == '__main__':
    unittest.main()
