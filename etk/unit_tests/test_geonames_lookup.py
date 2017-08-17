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
                    "state_to_codes_lower" : states_to_codes_path,
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

        ex_populated_places = [
            {
                "value": "los angeles",
                "confidence": 1,
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
                "confidence": 1,
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
        ex_city_name_gt = [
            {
                "confidence": 1,
                "provenance": [
                    {
                        "source": {
                            "segment": "content_strict",
                            "context": {
                                "end": 90,
                                "tokens_left": [
                                    "'",
                                    "s",
                                    "location",
                                    ":",
                                    "\n"
                                ],
                                "text": "' s location : \n <etk 'attribute' = 'city_name'>los angeles</etk> , california \n escort ' ",
                                "start": 88,
                                "input": "tokens",
                                "tokens_right": [
                                    ",",
                                    "california",
                                    "\n",
                                    "escort",
                                    "'"
                                ]
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
                                "tokens_left": [
                                    "'",
                                    "s",
                                    "location",
                                    ":",
                                    "\n"
                                ],
                                "text": "' s location : \n <etk 'attribute' = 'city_name'>los angeles</etk> , california \n escort ' ",
                                "start": 88,
                                "input": "tokens",
                                "tokens_right": [
                                    ",",
                                    "california",
                                    "\n",
                                    "escort",
                                    "'"
                                ]
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
        ex_city_gt = [
            {
                "value": "los angeles,california-1.0",
                "confidence": 1,
                "provenance": [
                    {
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
                "qualifiers": {
                    "country": "united states",
                    "city_country_together": 0,
                    "city_country_separate": 0,
                    "longitude": -118.24368,
                    "city_state_together": 2,
                    "geoname_id": 5368361,
                    "state": "california",
                    "latitude": 34.05223,
                    "city_state_separate": 2,
                    "city_state_code_separate": 0,
                    "city_state_code_together": 0,
                    "population": 3792621
                },
                "key": "los angeles:california:united states:-118.24368:34.05223"
            }
        ]
        ex_states_usa_codes_gt = [
            {
                "confidence": 1,
                "provenance": [
                    {
                        "source": {
                            "segment": "content_strict",
                            "context": {
                                "end": 18,
                                "tokens_left": [
                                    "160cms",
                                    "weight",
                                    "55",
                                    "kilos",
                                    "contact"
                                ],
                                "text": "160cms weight 55 kilos contact <etk 'attribute' = 'states_usa_codes'>me</etk> at escort . here @ ",
                                "start": 17,
                                "input": "tokens",
                                "tokens_right": [
                                    "at",
                                    "escort",
                                    ".",
                                    "here",
                                    "@"
                                ]
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
                                "end": 194,
                                "tokens_left": [
                                    "-",
                                    "452",
                                    "-",
                                    "2013",
                                    "."
                                ],
                                "text": "- 452 - 2013 . <etk 'attribute' = 'states_usa_codes'>me</etk> and my friends are on ",
                                "start": 193,
                                "input": "tokens",
                                "tokens_right": [
                                    "and",
                                    "my",
                                    "friends",
                                    "are",
                                    "on"
                                ]
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
                                "end": 218,
                                "tokens_left": [
                                    ".",
                                    ".",
                                    "skittlegirl",
                                    "\n\n\n\n\n\n",
                                    "call"
                                ],
                                "text": ". . skittlegirl \n\n\n\n\n\n call <etk 'attribute' = 'states_usa_codes'>me</etk> on my cell at 323 ",
                                "start": 217,
                                "input": "tokens",
                                "tokens_right": [
                                    "on",
                                    "my",
                                    "cell",
                                    "at",
                                    "323"
                                ]
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
                                "end": 18,
                                "tokens_left": [
                                    "160cms",
                                    "weight",
                                    "55",
                                    "kilos",
                                    "contact"
                                ],
                                "text": "160cms weight 55 kilos contact <etk 'attribute' = 'states_usa_codes'>me</etk> at escort . here @ ",
                                "start": 17,
                                "input": "tokens",
                                "tokens_right": [
                                    "at",
                                    "escort",
                                    ".",
                                    "here",
                                    "@"
                                ]
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
                                "end": 194,
                                "tokens_left": [
                                    "-",
                                    "452",
                                    "-",
                                    "2013",
                                    "."
                                ],
                                "text": "- 452 - 2013 . <etk 'attribute' = 'states_usa_codes'>me</etk> and my friends are on ",
                                "start": 193,
                                "input": "tokens",
                                "tokens_right": [
                                    "and",
                                    "my",
                                    "friends",
                                    "are",
                                    "on"
                                ]
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
                                "end": 218,
                                "tokens_left": [
                                    ".",
                                    ".",
                                    "skittlegirl",
                                    "\n\n\n\n\n\n",
                                    "call"
                                ],
                                "text": ". . skittlegirl \n\n\n\n\n\n call <etk 'attribute' = 'states_usa_codes'>me</etk> on my cell at 323 ",
                                "start": 217,
                                "input": "tokens",
                                "tokens_right": [
                                    "on",
                                    "my",
                                    "cell",
                                    "at",
                                    "323"
                                ]
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
                "value": "me",
                "key": "me"
            },
            {
                "confidence": 1,
                "provenance": [
                    {
                        "source": {
                            "segment": "content_strict",
                            "context": {
                                "end": 174,
                                "tokens_left": [
                                    "exactly",
                                    "what",
                                    "you",
                                    "deserve",
                                    "call"
                                ],
                                "text": "exactly what you deserve call <etk 'attribute' = 'states_usa_codes'>or</etk> text fetish friendly fantasy friendly ",
                                "start": 173,
                                "input": "tokens",
                                "tokens_right": [
                                    "text",
                                    "fetish",
                                    "friendly",
                                    "fantasy",
                                    "friendly"
                                ]
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
                                "end": 174,
                                "tokens_left": [
                                    "exactly",
                                    "what",
                                    "you",
                                    "deserve",
                                    "call"
                                ],
                                "text": "exactly what you deserve call <etk 'attribute' = 'states_usa_codes'>or</etk> text fetish friendly fantasy friendly ",
                                "start": 173,
                                "input": "tokens",
                                "tokens_right": [
                                    "text",
                                    "fetish",
                                    "friendly",
                                    "fantasy",
                                    "friendly"
                                ]
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
                "value": "or",
                "key": "or"
            },
            {
                "confidence": 1,
                "provenance": [
                    {
                        "source": {
                            "segment": "content_strict",
                            "context": {
                                "end": 127,
                                "tokens_left": [
                                    "there",
                                    "are",
                                    "50",
                                    "girls",
                                    "looking"
                                ],
                                "text": "there are 50 girls looking <etk 'attribute' = 'states_usa_codes'>in</etk> . \n view girls \n\n ",
                                "start": 126,
                                "input": "tokens",
                                "tokens_right": [
                                    ".",
                                    "\n",
                                    "view",
                                    "girls",
                                    "\n\n"
                                ]
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
                                "end": 147,
                                "tokens_left": [
                                    "explore",
                                    ",",
                                    "embrace",
                                    "and",
                                    "indulge"
                                ],
                                "text": "explore , embrace and indulge <etk 'attribute' = 'states_usa_codes'>in</etk> your favorite fantasy % independent ",
                                "start": 146,
                                "input": "tokens",
                                "tokens_right": [
                                    "your",
                                    "favorite",
                                    "fantasy",
                                    "%",
                                    "independent"
                                ]
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
                                "end": 127,
                                "tokens_left": [
                                    "there",
                                    "are",
                                    "50",
                                    "girls",
                                    "looking"
                                ],
                                "text": "there are 50 girls looking <etk 'attribute' = 'states_usa_codes'>in</etk> . \n view girls \n\n ",
                                "start": 126,
                                "input": "tokens",
                                "tokens_right": [
                                    ".",
                                    "\n",
                                    "view",
                                    "girls",
                                    "\n\n"
                                ]
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
                                "end": 147,
                                "tokens_left": [
                                    "explore",
                                    ",",
                                    "embrace",
                                    "and",
                                    "indulge"
                                ],
                                "text": "explore , embrace and indulge <etk 'attribute' = 'states_usa_codes'>in</etk> your favorite fantasy % independent ",
                                "start": 146,
                                "input": "tokens",
                                "tokens_right": [
                                    "your",
                                    "favorite",
                                    "fantasy",
                                    "%",
                                    "independent"
                                ]
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
                "value": "in",
                "key": "in"
            }
        ]
        pop_places = json.loads(json.JSONEncoder().encode(r['knowledge_graph']['populated_places']))
        ex_pop_places = json.loads(json.JSONEncoder().encode(ex_populated_places))
        self.assertEqual(pop_places, ex_pop_places)

        pop_city_name = json.loads(json.JSONEncoder().encode(r['knowledge_graph']['city_name']))
        ex_city_name = json.loads(json.JSONEncoder().encode(ex_city_name_gt))
        self.assertEqual(pop_city_name, ex_city_name)

        pop_city = json.loads(json.JSONEncoder().encode(r['knowledge_graph']['city']))
        ex_city = json.loads(json.JSONEncoder().encode(ex_city_gt))
        self.assertEqual(pop_city, ex_city)

        pop_states_usa_codes = json.loads(json.JSONEncoder().encode(r['knowledge_graph']['states_usa_codes']))
        ex_states_usa_codes = json.loads(json.JSONEncoder().encode(ex_states_usa_codes_gt))
        self.assertEqual(pop_states_usa_codes, ex_states_usa_codes)

        # self.assertTrue('geonames_country' in r['knowledge_graph'])
        #
        # ex_country = [
        #     {
        #         "confidence": 1000,
        #         "provenance": [
        #             {
        #                 "extracted_value": "united states"
        #             }
        #         ],
        #         "value": "united states",
        #         "key": "united states"
        #     },
        #     {
        #         "confidence": 1000,
        #         "provenance": [
        #             {
        #                 "extracted_value": "chile"
        #             }
        #         ],
        #         "value": "chile",
        #         "key": "chile"
        #     }
        # ]
        # self.assertEqual(r['knowledge_graph']['geonames_country'], ex_country)


if __name__ == '__main__':
    unittest.main()
