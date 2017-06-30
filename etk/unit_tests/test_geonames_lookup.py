# -*- coding: utf-8 -*-
import unittest
import sys
import os

sys.path.append('../../')
from etk.core import Core
import json
import codecs


class TestGeonamesLookup(unittest.TestCase):
    def setUp(self):
        file_path = os.path.join(os.path.dirname(__file__), "ground_truth/1_content_extracted.jl")
        self.doc = json.load(codecs.open(file_path, "r", "utf-8"))

    def test_geonames_lookup(self):
        cities_file_path = os.path.join(os.path.dirname(__file__), "resources/cities.json.gz")
        geonames_file_path = os.path.join(os.path.dirname(__file__), "resources/geonames.json")
        e_config = {
            "document_id": "doc_id",
            "resources": {
                "dictionaries": {
                    "geonames": geonames_file_path,
                    "city": cities_file_path
                }
            },
            "data_extraction": [
                {
                    "input_path": [
                        "*.content_strict.text.`parent`",
                        "*.content_relaxed.text.`parent`",
                    ],
                    "fields": {
                        "city_name": {
                            "extractors": {
                                "extract_using_dictionary": {
                                    "config": {
                                        "dictionary": "city",
                                        "ngrams": 2
                                    }
                                }
                            }
                        }
                    }
                }
            ],
            "kg_enhancement": {
                "input_path": "knowledge_graph.`parent`",
                "fields": {
                    "populated_places": {
                        "priority" : 0,
                        "extractors": {
                            "geonames_lookup": {
                                "config": {}
                            }
                        }
                    }
                }
            }
        }
        c = Core(extraction_config=e_config)
        r = c.process(self.doc, create_knowledge_graph=True)

        # with codecs.open("kg_out.jl", "w", "utf-8") as f:
        #     f.write(json.dumps(r))

        self.assertTrue('knowledge_graph' in r)
        self.assertTrue('populated_places' in r['knowledge_graph'])

        ex_populated_places = [
          {
            "value": "los angeles",
            "confidence": 1000,
            "provenance": [
              {
                "extracted_value": "los angeles"
              }
            ],
            "qualifiers": {
              "country": "united states",
              "longitude": -118.24368,
              "geoname_id": 5368361,
              "state": "california",
              "latitude": 34.05223,
              "population": 3792621
            },
            "key": "los angeles-country:united states-geoname_id:5368361-latitude:34.05223-longitude:-118.24368-population:3792621-state:california"
          },
          {
            "value": "los angeles",
            "confidence": 1000,
            "provenance": [
              {
                "extracted_value": "los angeles"
              }
            ],
            "qualifiers": {
              "country": "chile",
              "longitude": -72.35365999999999,
              "geoname_id": 3882428,
              "state": "biobío",
              "latitude": -37.46973,
              "population": 125430
            },
            "key": "los angeles-country:chile-geoname_id:3882428-latitude:-37.46973-longitude:-72.35366-population:125430-state:biobío"
          }
        ]

        pop_places = json.loads(json.JSONEncoder().encode(r['knowledge_graph']['populated_places']))
        ex_pop_places = json.loads(json.JSONEncoder().encode(ex_populated_places))
        self.assertEqual(pop_places, ex_pop_places)

        self.assertTrue('geonames_country' in r['knowledge_graph'])

        ex_country = [
          {
            "confidence": 1000,
            "provenance": [
              {
                "extracted_value": "united states"
              }
            ],
            "value": "united states",
            "key": "united states"
          },
          {
            "confidence": 1000,
            "provenance": [
              {
                "extracted_value": "chile"
              }
            ],
            "value": "chile",
            "key": "chile"
          }
        ]
        self.assertEqual(r['knowledge_graph']['geonames_country'], ex_country)


if __name__ == '__main__':
    unittest.main()
