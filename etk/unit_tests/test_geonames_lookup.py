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
        states_to_codes_path = os.path.join(os.path.dirname(__file__), "resources/states-to-codes.json")
        populated_cities_path = os.path.join(os.path.dirname(__file__), "resources/populated_cities_dict.json")
        states_usa_canada_path = os.path.join(os.path.dirname(__file__), "resources/states_usa_canada.json.gz")
        states_usa_codes_path = os.path.join(os.path.dirname(__file__), "resources/states_usa_codes.json.gz")
        country_path = os.path.join(os.path.dirname(__file__), "resources/countries.json.gz")
        e_config = {
            "document_id": "doc_id",
            "resources": {
                "dictionaries": {
                    "geonames": geonames_file_path,
                    "city": cities_file_path,
                    "state_to_codes_lower": states_to_codes_path,
                    "populated_cities": populated_cities_path,
                    "states_usa_canada": states_usa_canada_path,
                    "states_usa_codes": states_usa_codes_path,
                    "countries": country_path
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
                        "city_name": {
                            "extractors": {
                                "extract_using_dictionary": {
                                    "config": {
                                        "dictionary": "city",
                                        "ngrams": 2
                                    }
                                }
                            }
                        },
                        "state": {
                            "extractors": {
                                "extract_using_dictionary": {
                                    "config": {
                                        "ngrams": 3,
                                        "dictionary": "states_usa_canada"
                                    }
                                }
                            }
                        },
                        "states_usa_codes": {
                            "extractors": {
                                "extract_using_dictionary": {
                                    "config": {
                                        "ngrams": 1,
                                        "dictionary": "states_usa_codes"
                                    }
                                }
                            }
                        },
                        "country": {
                            "extractors": {
                                "extract_using_dictionary": {
                                    "config": {
                                        "dictionary": "countries"
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
                        "priority": 0,
                        "extractors": {
                            "geonames_lookup": {
                                "config": {}
                            }
                        }
                    },
                    "city": {
                        "priority": 1,
                        "extractors": {
                            "create_city_state_country_triple": {
                                "config": {}
                            }
                        }
                    }
                }
            }
        }
        c = Core(extraction_config=e_config)
        r = c.process(self.doc, create_knowledge_graph=True)

        self.assertTrue('knowledge_graph' in r)
        self.assertTrue('populated_places' in r['knowledge_graph'])

        ex_populated_places = """[
          {
            "confidence": 1,
            "provenance": [
              {
                "qualifiers": {
                  "country": "chile",
                  "longitude": -72.35365999999999,
                  "geoname_id": 3882428,
                  "state": "biob\u00edo",
                  "latitude": -37.46973,
                  "population": 125430
                },
                "source": {
                  "segment": "post_process",
                  "document_id": "1A4A5FF5BD066309C72C8EEE6F7BCCCFD21B83245AFCDADDF014455BCF990A21"
                },
                "confidence": {
                  "extraction": 1.0
                },
                "method": "geonames_lookup",
                "extracted_value": "los angeles"
              }
            ],
            "key": "los angeles-country:chile-geoname_id:3882428-latitude:-37.46973-longitude:-72.35366-population:125430-state:biob\u00edo",
            "value": "los angeles"
          },
          {
            "confidence": 1,
            "provenance": [
              {
                "qualifiers": {
                  "country": "united states",
                  "longitude": -118.24368,
                  "geoname_id": 5368361,
                  "state": "california",
                  "latitude": 34.05223,
                  "population": 3792621
                },
                "source": {
                  "segment": "post_process",
                  "document_id": "1A4A5FF5BD066309C72C8EEE6F7BCCCFD21B83245AFCDADDF014455BCF990A21"
                },
                "confidence": {
                  "extraction": 1.0
                },
                "method": "geonames_lookup",
                "extracted_value": "los angeles"
              }
            ],
            "key": "los angeles-country:united states-geoname_id:5368361-latitude:34.05223-longitude:-118.24368-population:3792621-state:california",
            "value": "los angeles"
          }
        ]"""
        ex_city_name_gt = [
            {
                "confidence": 1,
                "provenance": [
                    {
                        "source": {
                            "segment": "content_strict",
                            "context": {
                                "end": 90,
                                "text": "' s location : \n <etk 'attribute' = 'city_name'>los angeles</etk> , california \n escort ' ",
                                "start": 88,
                                "input": "tokens"
                            },
                            "document_id": "1A4A5FF5BD066309C72C8EEE6F7BCCCFD21B83245AFCDADDF014455BCF990A21"
                        },
                        "confidence": {
                            "extraction": 1.0
                        },
                        "method": "extract_using_dictionary",
                        "extracted_value": "los angeles"
                    },
                    {
                        "source": {
                            "segment": "content_relaxed",
                            "context": {
                                "end": 90,
                                "text": "' s location : \n <etk 'attribute' = 'city_name'>los angeles</etk> , california \n escort ' ",
                                "start": 88,
                                "input": "tokens"
                            },
                            "document_id": "1A4A5FF5BD066309C72C8EEE6F7BCCCFD21B83245AFCDADDF014455BCF990A21"
                        },
                        "confidence": {
                            "extraction": 1.0
                        },
                        "method": "extract_using_dictionary",
                        "extracted_value": "los angeles"
                    }
                ],
                "value": "los angeles",
                "key": "los angeles"
            }
        ]
        ex_city_gt = """[
                {
                    "confidence": 1,
                    "provenance": [
                        {
                            "qualifiers": {
                                "city_country_together": 0,
                                "city_country_separate": 0,
                                "longitude": -118.24368,
                                "city_state_together": 2,
                                "latitude": 34.05223,
                                "city_state_separate": 2,
                                "city_state_code_separate": 0,
                                "city_state_code_together": 0,
                                "population": 3792621
                            },
                            "source": {
                                "segment": "city_state_together in content_strict content_relaxed",
                                "document_id": "1A4A5FF5BD066309C72C8EEE6F7BCCCFD21B83245AFCDADDF014455BCF990A21"
                            },
                            "confidence": {
                                "extraction": 1.0
                            },
                            "method": "create_city_state_country_triple",
                            "extracted_value": "los angeles,california-1.0"
                        }
                    ],
                    "key": "los angeles:california:united states:-118.24368:34.05223",
                    "value": "los angeles,california-1.0"
                }
            ]"""
        ex_states_usa_codes_gt = [
          {
            "confidence": 1,
            "provenance": [
              {
                "source": {
                  "segment": "content_strict",
                  "context": {
                    "start": 17,
                    "end": 18,
                    "input": "tokens",
                    "text": "160cms weight 55 kilos contact <etk 'attribute' = 'states_usa_codes'>me</etk> at escort . here @ "
                  },
                  "document_id": "1A4A5FF5BD066309C72C8EEE6F7BCCCFD21B83245AFCDADDF014455BCF990A21"
                },
                "confidence": {
                  "extraction": 1.0
                },
                "method": "extract_using_dictionary",
                "extracted_value": "me"
              },
              {
                "source": {
                  "segment": "content_strict",
                  "context": {
                    "start": 193,
                    "end": 194,
                    "input": "tokens",
                    "text": "- 452 - 2013 . <etk 'attribute' = 'states_usa_codes'>me</etk> and my friends are on "
                  },
                  "document_id": "1A4A5FF5BD066309C72C8EEE6F7BCCCFD21B83245AFCDADDF014455BCF990A21"
                },
                "confidence": {
                  "extraction": 1.0
                },
                "method": "extract_using_dictionary",
                "extracted_value": "me"
              },
              {
                "source": {
                  "segment": "content_strict",
                  "context": {
                    "start": 217,
                    "end": 218,
                    "input": "tokens",
                    "text": ". . skittlegirl \n\n\n\n\n\n call <etk 'attribute' = 'states_usa_codes'>me</etk> on my cell at 323 "
                  },
                  "document_id": "1A4A5FF5BD066309C72C8EEE6F7BCCCFD21B83245AFCDADDF014455BCF990A21"
                },
                "confidence": {
                  "extraction": 1.0
                },
                "method": "extract_using_dictionary",
                "extracted_value": "me"
              },
              {
                "source": {
                  "segment": "content_relaxed",
                  "context": {
                    "start": 17,
                    "end": 18,
                    "input": "tokens",
                    "text": "160cms weight 55 kilos contact <etk 'attribute' = 'states_usa_codes'>me</etk> at escort . here @ "
                  },
                  "document_id": "1A4A5FF5BD066309C72C8EEE6F7BCCCFD21B83245AFCDADDF014455BCF990A21"
                },
                "confidence": {
                  "extraction": 1.0
                },
                "method": "extract_using_dictionary",
                "extracted_value": "me"
              },
              {
                "source": {
                  "segment": "content_relaxed",
                  "context": {
                    "start": 193,
                    "end": 194,
                    "input": "tokens",
                    "text": "- 452 - 2013 . <etk 'attribute' = 'states_usa_codes'>me</etk> and my friends are on "
                  },
                  "document_id": "1A4A5FF5BD066309C72C8EEE6F7BCCCFD21B83245AFCDADDF014455BCF990A21"
                },
                "confidence": {
                  "extraction": 1.0
                },
                "method": "extract_using_dictionary",
                "extracted_value": "me"
              },
              {
                "source": {
                  "segment": "content_relaxed",
                  "context": {
                    "start": 217,
                    "end": 218,
                    "input": "tokens",
                    "text": ". . skittlegirl \n\n\n\n\n\n call <etk 'attribute' = 'states_usa_codes'>me</etk> on my cell at 323 "
                  },
                  "document_id": "1A4A5FF5BD066309C72C8EEE6F7BCCCFD21B83245AFCDADDF014455BCF990A21"
                },
                "confidence": {
                  "extraction": 1.0
                },
                "method": "extract_using_dictionary",
                "extracted_value": "me"
              }
            ],
            "key": "me",
            "value": "me"
          },
          {
            "confidence": 1,
            "provenance": [
              {
                "source": {
                  "segment": "content_strict",
                  "context": {
                    "start": 126,
                    "end": 127,
                    "input": "tokens",
                    "text": "there are 50 girls looking <etk 'attribute' = 'states_usa_codes'>in</etk> . \n view girls \n\n "
                  },
                  "document_id": "1A4A5FF5BD066309C72C8EEE6F7BCCCFD21B83245AFCDADDF014455BCF990A21"
                },
                "confidence": {
                  "extraction": 1.0
                },
                "method": "extract_using_dictionary",
                "extracted_value": "in"
              },
              {
                "source": {
                  "segment": "content_strict",
                  "context": {
                    "start": 146,
                    "end": 147,
                    "input": "tokens",
                    "text": "explore , embrace and indulge <etk 'attribute' = 'states_usa_codes'>in</etk> your favorite fantasy % independent "
                  },
                  "document_id": "1A4A5FF5BD066309C72C8EEE6F7BCCCFD21B83245AFCDADDF014455BCF990A21"
                },
                "confidence": {
                  "extraction": 1.0
                },
                "method": "extract_using_dictionary",
                "extracted_value": "in"
              },
              {
                "source": {
                  "segment": "content_relaxed",
                  "context": {
                    "start": 126,
                    "end": 127,
                    "input": "tokens",
                    "text": "there are 50 girls looking <etk 'attribute' = 'states_usa_codes'>in</etk> . \n view girls \n\n "
                  },
                  "document_id": "1A4A5FF5BD066309C72C8EEE6F7BCCCFD21B83245AFCDADDF014455BCF990A21"
                },
                "confidence": {
                  "extraction": 1.0
                },
                "method": "extract_using_dictionary",
                "extracted_value": "in"
              },
              {
                "source": {
                  "segment": "content_relaxed",
                  "context": {
                    "start": 146,
                    "end": 147,
                    "input": "tokens",
                    "text": "explore , embrace and indulge <etk 'attribute' = 'states_usa_codes'>in</etk> your favorite fantasy % independent "
                  },
                  "document_id": "1A4A5FF5BD066309C72C8EEE6F7BCCCFD21B83245AFCDADDF014455BCF990A21"
                },
                "confidence": {
                  "extraction": 1.0
                },
                "method": "extract_using_dictionary",
                "extracted_value": "in"
              }
            ],
            "key": "in",
            "value": "in"
          },
          {
            "confidence": 1,
            "provenance": [
              {
                "source": {
                  "segment": "content_strict",
                  "context": {
                    "start": 173,
                    "end": 174,
                    "input": "tokens",
                    "text": "exactly what you deserve call <etk 'attribute' = 'states_usa_codes'>or</etk> text fetish friendly fantasy friendly "
                  },
                  "document_id": "1A4A5FF5BD066309C72C8EEE6F7BCCCFD21B83245AFCDADDF014455BCF990A21"
                },
                "confidence": {
                  "extraction": 1.0
                },
                "method": "extract_using_dictionary",
                "extracted_value": "or"
              },
              {
                "source": {
                  "segment": "content_relaxed",
                  "context": {
                    "start": 173,
                    "end": 174,
                    "input": "tokens",
                    "text": "exactly what you deserve call <etk 'attribute' = 'states_usa_codes'>or</etk> text fetish friendly fantasy friendly "
                  },
                  "document_id": "1A4A5FF5BD066309C72C8EEE6F7BCCCFD21B83245AFCDADDF014455BCF990A21"
                },
                "confidence": {
                  "extraction": 1.0
                },
                "method": "extract_using_dictionary",
                "extracted_value": "or"
              }
            ],
            "key": "or",
            "value": "or"
          }
        ]
        self.assertEqual(r['knowledge_graph']['populated_places'], json.loads(ex_populated_places))

        ex_city_name = json.loads(json.JSONEncoder().encode(ex_city_name_gt))
        self.assertEqual(r['knowledge_graph']['city_name'], ex_city_name)

        self.assertEqual(r['knowledge_graph']['city'], json.loads(ex_city_gt))

        pop_states_usa_codes = json.loads(json.JSONEncoder().encode(r['knowledge_graph']['states_usa_codes']))
        self.assertEqual(len(pop_states_usa_codes), len(ex_states_usa_codes_gt))
        self.assertEqual(pop_states_usa_codes, ex_states_usa_codes_gt)


if __name__ == '__main__':
    unittest.main()
