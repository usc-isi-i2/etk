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
        e_config = {
            "resources": {
                "dictionaries": {
                    "geonames": "/home/vinay/Documents/Study/ISI/dig-dictionaries/geonames-populated-places/city_dict_alt_15000.json",
                    "city": "/home/vinay/Documents/Study/ISI/dig-dictionaries/geonames-populated-places/curated_cities.json.gz"
                }
            },
            "data_extraction": [
                {
                    "input_path": [
                        "*.content_strict.text.`parent`",
                        "*.content_relaxed.text.`parent`",
                    ],
                    "fields": {
                        "city": {
                            "extractors": {
                                "extract_using_dictionary": {
                                    "config": {
                                        "dictionary": "city"
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
        # f.write(json.dumps(r))

        self.assertTrue('knowledge_graph' in r)
        self.assertTrue('populated_places' in r['knowledge_graph'])

        ex_populated_places = [
            {
                "context": {},
                "value": "angeles-biobío-chile",
                "metadata": {
                    "country": "chile",
                    "longitude": -72.35365999999999,
                    "geoname_id": 3882428,
                    "state": "biobío",
                    "latitude": -37.46973,
                    "population": 125430
                }
            },
            {
                "context": {},
                "value": "angeles-central luzon-philippines",
                "metadata": {
                    "country": "philippines",
                    "longitude": 120.58333,
                    "geoname_id": 1730737,
                    "state": "central luzon",
                    "latitude": 15.15,
                    "population": 299391
                }
            },
            {
                "context": {},
                "value": "helena-alabama-united states",
                "metadata": {
                    "country": "united states",
                    "longitude": -86.8436,
                    "geoname_id": 4066811,
                    "state": "alabama",
                    "latitude": 33.29622,
                    "population": 16793
                }
            },
            {
                "context": {},
                "value": "helena-montana-united states",
                "metadata": {
                    "country": "united states",
                    "longitude": -112.03611000000001,
                    "geoname_id": 5656882,
                    "state": "montana",
                    "latitude": 46.59271,
                    "population": 28190
                }
            },
            {
                "context": {},
                "value": "i-wallonia-belgium",
                "metadata": {
                    "country": "belgium",
                    "longitude": 5.23284,
                    "geoname_id": 2795113,
                    "state": "wallonia",
                    "latitude": 50.51894,
                    "population": 19973
                }
            },
            {
                "context": {},
                "value": "i-shandong-china",
                "metadata": {
                    "country": "china",
                    "longitude": 119.94216999999999,
                    "geoname_id": 1804578,
                    "state": "shandong",
                    "latitude": 37.18073,
                    "population": 90070
                }
            }
        ]

        pop_places = json.loads(json.JSONEncoder().encode(r['knowledge_graph']['populated_places']))
        ex_pop_places = json.loads(json.JSONEncoder().encode(ex_populated_places))
        self.assertEqual(pop_places, ex_pop_places)


if __name__ == '__main__':
    unittest.main()
