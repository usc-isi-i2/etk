import unittest
import sys
import os
sys.path.append('../../')
from etk.core import Core
import json
import codecs


class TestNestedDocs(unittest.TestCase):

    def setUp(self):
        self.e_config = {
            "document_id": "doc_id",
            "extraction_policy": "replace",
            "error_handling": "raise_error",
            "resources": {
                "dictionaries": {

                },
                "landmark": [
                ],
                "pickle": {
                }
            },
            "content_extraction": {
                "input_path": "raw_content",
                "extractors": {
                    "table": {
                        "field_name": "table",
                        "config": {
                        },
                        "extraction_policy": "replace"
                    }
                }
            },
            "data_extraction": [
                {
                    "input_path": [
                        "$.content_extraction.table_row.cells[*]"
                    ],
                    "guards": [
                        {
                            "type": "doc",
                            "path": "$.url",
                            "regex": "xx.com"
                        },
                        {
                            "type": "doc",
                            "path": "$.@type",
                            "regex": "table_row_doc"
                        }
                    ],
                    "fields": {
                        "event_date": {
                            "extractors": {
                                "extract_as_is": {
                                    "config": {
                                        "post_filter": "parse_date"
                                    }
                                }
                            }
                        }
                    }
                }
            ],
            "kg_enhancement": [

            ]
        }
        self.doc1 =  {
          "@type":"table_row_doc",
          "content_extraction":{
             "table_row":{
                "cells":[
                   {
                      "text":"S/RES/2321   \n (2016)",
                      "cell":"<td class=\"class\"><a href=\"http://www.un.org/en/ga/search/view_doc.asp?symbol=S/RES/2321(2016)\">S/RES/2321 \n      (2016)</a></td>"
                   },
                   {
                      "text":"30 November 2016",
                      "cell":"<td class=\"class\">30 November 2016</td>"
                   },
                   {
                      "text":"Non-proliferation/Democratic People Republic of Korea",
                      "cell":"<td class=\"\">Non-proliferation/Democratic People Republic of Korea</td>"
                   }
                ]
             }
          },
          "document_id":"1C3EE04533D60FDF32CAC9A49D4A1B040DDF9CD3F1A543F2DA15B3AA1A79AD3E",
          "type":"test_table3",
          "created_by":"etk",
          "tld":"xx.com",
          "doc_id":"1C3EE04533D60FDF32CAC9A49D4A1B040DDF9CD3F1A543F2DA15B3AA1A79AD3E",
          "url":"xx.com",
          "@timestamp":"2018-01-19T21:32:36.824Z",
          "parent_doc_id":"xx",
          "prefilter_filter_outcome":"no_action",
          "@timestamp_created":"2018-01-19T21:32:09.124495",
          "@version":"1",
          "@execution_profile":{
             "@run_core_time":1.2515268325805664,
             "@worker_id":0,
             "@doc_sent_time":"2018-01-19T21:32:36.821271",
             "@etk_process_time":1.2514488697052002,
             "@doc_length":1324,
             "@etk_start_time":"2018-01-19T21:32:35.569749",
             "@doc_arrived_time":"2018-01-19T21:32:01.623305",
             "@doc_wait_time":0.0,
             "@etk_end_time":"2018-01-19T21:32:36.821198"
          }
        }
        self.doc2 = {
            "@type": "not_table_row_doc",
            "content_extraction": {
                "table_row": {
                    "cells": [
                        {
                            "text": "S/RES/2321   \n (2016)",
                            "cell": "<td class=\"class\"><a href=\"http://www.un.org/en/ga/search/view_doc.asp?symbol=S/RES/2321(2016)\">S/RES/2321 \n      (2016)</a></td>"
                        },
                        {
                            "text": "30 November 2016",
                            "cell": "<td class=\"class\">30 November 2016</td>"
                        },
                        {
                            "text": "Non-proliferation/Democratic People Republic of Korea",
                            "cell": "<td class=\"\">Non-proliferation/Democratic People Republic of Korea</td>"
                        }
                    ]
                }
            },
            "document_id": "1C3EE04533D60FDF32CAC9A49D4A1B040DDF9CD3F1A543F2DA15B3AA1A79AD3E",
            "type": "test_table3",
            "created_by": "etk",
            "tld": "xx.com",
            "doc_id": "1C3EE04533D60FDF32CAC9A49D4A1B040DDF9CD3F1A543F2DA15B3AA1A79AD3E",
            "url": "xx.com",
            "@timestamp": "2018-01-19T21:32:36.824Z",
            "parent_doc_id": "xx",
            "prefilter_filter_outcome": "no_action",
            "@timestamp_created": "2018-01-19T21:32:09.124495",
            "@version": "1",
            "@execution_profile": {
                "@run_core_time": 1.2515268325805664,
                "@worker_id": 0,
                "@doc_sent_time": "2018-01-19T21:32:36.821271",
                "@etk_process_time": 1.2514488697052002,
                "@doc_length": 1324,
                "@etk_start_time": "2018-01-19T21:32:35.569749",
                "@doc_arrived_time": "2018-01-19T21:32:01.623305",
                "@doc_wait_time": 0.0,
                "@etk_end_time": "2018-01-19T21:32:36.821198"
            }
        }
        file_path = os.path.join(os.path.dirname(__file__), "ground_truth/table.jl")
        table_out = os.path.join(os.path.dirname(__file__), "ground_truth/table_out.jl")
        no_table_file = os.path.join(os.path.dirname(__file__), "ground_truth/1_content_extracted.jl")
        self.doc = json.load(codecs.open(file_path, "r", "utf-8"))
        self.table_ex = json.load(codecs.open(table_out, "r", "utf-8"))
        self.no_table = json.load(codecs.open(no_table_file, "r", "utf-8"))

    def test_guards(self):
        c = Core(extraction_config=self.e_config)
        r = c.process(self.doc1)
        self.assertTrue("knowledge_graph" in r)
        self.assertTrue("event_date" in r['knowledge_graph'])
        r = c.process(self.doc2)
        self.assertTrue("knowledge_graph" not in r or "event_date" not in r['knowledge_graph'])

if __name__ == '__main__':
    unittest.main()
