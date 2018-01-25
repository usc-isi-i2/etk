# -*- coding: utf-8 -*-
import unittest
import sys, os

sys.path.append('../../')
from etk.core import Core
import json
import codecs


class TestExtractions(unittest.TestCase):
    def setUp(self):
        file_path = os.path.join(os.path.dirname(__file__), "ground_truth/1.jl")
        self.doc = json.load(codecs.open(file_path, 'r'))

    def test_no_config(self):
        e_config = None
        c = Core(extraction_config=e_config)
        r = c.process(self.doc)
        self.assertTrue(r)
        self.assertTrue("content_extraction" not in r)

    def test_ce_no_inputpath(self):
        e_config = {'content_extraction': {'extractors': {'title': {}}}}
        c = Core(extraction_config=e_config)
        with self.assertRaises(KeyError):
            r = c.process(self.doc)

    def test_ce_readability(self):
        e_config = {'content_extraction': {
            "input_path": "raw_content",
            "extractors": {
                "readability": [
                    {
                        "strict": "yes",
                        "extraction_policy": "keep_existing"
                    },
                    {
                        "strict": "no",
                        "extraction_policy": "keep_existing",
                        "field_name": "content_relaxed"
                    }
                ]
            }
        }
        }
        c = Core(extraction_config=e_config)
        r = c.process(self.doc)
        self.assertTrue('tld' in r)
        self.assertEqual('eroticmugshots.com', r['tld'])
        self.assertTrue("content_extraction" in r)
        self.assertTrue("content_strict" in r["content_extraction"])
        self.assertTrue("content_relaxed" in r["content_extraction"])
        self.assertTrue("title" not in r["content_extraction"])
        self.assertTrue("inferlink_extractions" not in r["content_extraction"])

        c_s = """\n \n \n \n \n \n smoothlegs24  28 \n \n \n chrissy391  27 \n \n \n My name is Helena height 160cms weight 55 kilos  contact me at escort.here@gmail.com           jefferson ave         age: 23 HrumpMeNow  28 \n \n \n xxtradition  24 \n \n \n jumblyjumb  26 \n \n \n claudia77  26 \n \n \n gushinPuss  28 \n \n \n Littlexdit  25 \n \n \n PinkSweets2  28 \n \n \n withoutlimit  27 \n \n \n bothOfUs3  28 \n \n \n lovelylips  27 \n \n \n killerbod  27 \n \n \n Littlexdit  27 \n \n \n azneyes  23 \n \n \n \n \n \n Escort's Phone: \n \n \n323-452-2013  \n \n Escort's Location: \nLos Angeles, California  \n Escort's Age:   23   Date of Escort Post:   Jan 02nd 6:46am \n REVIEWS:   \n READ AND CREATE REVIEWS FOR THIS ESCORT   \n \n \n \n \n \nThere are  50  girls looking in  .\n VIEW GIRLS \n \nHey I'm luna 3234522013 Let's explore , embrace and indulge in your favorite fantasy  % independent. discreet no drama Firm Thighs and Sexy. My Soft skin & Tight Grip is exactly what you deserve Call or text   Fetish friendly   Fantasy friendly   Party friendly 140 Hr SPECIALS 3234522013. Call  323-452-2013 .  Me and my friends are on EZsex  soooo you can find us all on there if you want... skittlegirl \n   \n \n   \n \n   \n Call me on my cell at 323-452-2013. Date of ad: 2017-01-02 06:46:00 \n \n \n"""
        c_r = """\n \n \n \n \n \n \n smoothlegs24  28 \n \n \n chrissy391  27 \n \n \n My name is Helena height 160cms weight 55 kilos  contact me at escort.here@gmail.com           jefferson ave         age: 23 HrumpMeNow  28 \n \n \n xxtradition  24 \n \n \n jumblyjumb  26 \n \n \n claudia77  26 \n \n \n gushinPuss  28 \n \n \n Littlexdit  25 \n \n \n PinkSweets2  28 \n \n \n withoutlimit  27 \n \n \n bothOfUs3  28 \n \n \n lovelylips  27 \n \n \n killerbod  27 \n \n \n Littlexdit  27 \n \n \n azneyes  23 \n \n \n \n \n \n Escort's Phone: \n \n \n323-452-2013  \n \n Escort's Location: \nLos Angeles, California  \n Escort's Age:   23   Date of Escort Post:   Jan 02nd 6:46am \n REVIEWS:   \n READ AND CREATE REVIEWS FOR THIS ESCORT   \n \n \n \n \n \nThere are  50  girls looking in  .\n VIEW GIRLS \n \nHey I'm luna 3234522013 Let's explore , embrace and indulge in your favorite fantasy  % independent. discreet no drama Firm Thighs and Sexy. My Soft skin & Tight Grip is exactly what you deserve Call or text   Fetish friendly   Fantasy friendly   Party friendly 140 Hr SPECIALS 3234522013. Call  323-452-2013 .  Me and my friends are on EZsex  soooo you can find us all on there if you want... skittlegirl \n   \n \n   \n \n   \n Call me on my cell at 323-452-2013. Date of ad: 2017-01-02 06:46:00 \n \n \n \n"""
        self.assertEqual(json.dumps(r["content_extraction"]["content_strict"]["text"]), json.dumps(c_s))
        self.assertEqual(json.dumps(r["content_extraction"]["content_relaxed"]["text"]), json.dumps(c_r))

    def test_title(self):
        e_config = {'content_extraction': {
            "input_path": "raw_content",
            "extractors": {
                "title": {
                    "extraction_policy": "keep_existing"
                }
            }
        }
        }
        c = Core(extraction_config=e_config)
        r = c.process(self.doc)
        self.assertTrue("content_extraction" in r)
        self.assertTrue("title" in r["content_extraction"])
        title = """323-452-2013 ESCORT ALERT! - Luna The Hot Playmate (323) 452-2013 - 23"""
        self.assertEqual(r["content_extraction"]["title"]["text"], title)

        self.assertTrue("content_strict" not in r["content_extraction"])
        self.assertTrue("content_relaxed" not in r["content_extraction"])
        self.assertTrue("inferlink_extractions" not in r["content_extraction"])

    def test_landmark_no_resources(self):
        e_config = {'content_extraction': {
            "input_path": "raw_content",
            "extractors": {
                "landmark": {
                    "field_name": "inferlink_extractions",
                    "extraction_policy": "keep_existing",
                    "landmark_threshold": 0.5
                }
            }
        }
        }
        c = Core(extraction_config=e_config)
        with self.assertRaises(KeyError):
            r = c.process(self.doc)

    def test_landmark_with_field_name(self):
        rules_file_path = os.path.join(os.path.dirname(__file__), "resources/consolidated_rules.json")
        e_config = {"resources": {
            "landmark": [
                rules_file_path
            ]
        }, 'content_extraction': {
            "input_path": "raw_content",
            "extractors": {
                "landmark": {
                    "field_name": "inferlink_extractions",
                    "extraction_policy": "keep_existing",
                    "landmark_threshold": 0.5
                }
            }
        }
        }
        c = Core(extraction_config=e_config)
        r = c.process(self.doc)
        self.assertTrue("content_extraction" in r)
        self.assertTrue("inferlink_extractions" in r["content_extraction"])
        self.assertTrue(len(r["content_extraction"]["inferlink_extractions"].keys()) > 0)

        self.assertTrue("content_strict" not in r["content_extraction"])
        self.assertTrue("content_relaxed" not in r["content_extraction"])
        self.assertTrue("title" not in r["content_extraction"])

        ifl_extractions = {
            "inferlink_location": {
                "text": "Los Angeles, California"
            },
            "inferlink_age": {
                "text": "23"
            },
            "inferlink_phone": {
                "text": "323-452-2013"
            },
            "inferlink_description": {
                "text": "Hey I'm luna 3234522013 Let's explore , embrace and indulge in your favorite fantasy % independent. discreet no drama Firm Thighs and Sexy. My Soft skin & Tight Grip is exactly what you deserve Call or text Fetish friendly Fantasy friendly Party friendly 140 Hr SPECIALS 3234522013\n"
            },
            "inferlink_posting-date": {
                "text": "2017-01-02 06:46"
            }
        }

        self.assertEqual(r["content_extraction"]["inferlink_extractions"], ifl_extractions)

    def test_landmark_no_field_name(self):
        rules_file_path = os.path.join(os.path.dirname(__file__), "resources/consolidated_rules.json")
        e_config = {"resources": {
            "landmark": [
                rules_file_path
            ]
        }, 'content_extraction': {
            "input_path": "raw_content",
            "extractors": {
                "landmark": {
                    "extraction_policy": "keep_existing",
                    "landmark_threshold": 0.5
                }
            }
        }
        }
        c = Core(extraction_config=e_config)
        r = c.process(self.doc)
        self.assertTrue("content_extraction" in r)
        self.assertTrue("inferlink_extractions" in r["content_extraction"])
        self.assertTrue(len(r["content_extraction"]["inferlink_extractions"].keys()) > 0)

        ifl_extractions = {
            "inferlink_location": {
                "text": "Los Angeles, California"
            },
            "inferlink_age": {
                "text": "23"
            },
            "inferlink_phone": {
                "text": "323-452-2013"
            },
            "inferlink_description": {
                "text": "Hey I'm luna 3234522013 Let's explore , embrace and indulge in your favorite fantasy % independent. discreet no drama Firm Thighs and Sexy. My Soft skin & Tight Grip is exactly what you deserve Call or text Fetish friendly Fantasy friendly Party friendly 140 Hr SPECIALS 3234522013\n"
            },
            "inferlink_posting-date": {
                "text": "2017-01-02 06:46"
            }
        }
        self.assertEqual(r["content_extraction"]["inferlink_extractions"], ifl_extractions)

        self.assertTrue("content_strict" not in r["content_extraction"])
        self.assertTrue("content_relaxed" not in r["content_extraction"])
        self.assertTrue("title" not in r["content_extraction"])

    def test_content_extractions(self):
        rules_file_path = os.path.join(os.path.dirname(__file__), "resources/consolidated_rules.json")
        e_config = {"resources": {
            "landmark": [
                rules_file_path
            ]
        }, 'content_extraction': {
            "input_path": "raw_content",
            "extractors": {
                "title": {
                    "extraction_policy": "keep_existing"
                },
                "landmark": {
                    "extraction_policy": "keep_existing",
                    "landmark_threshold": 0.5
                },
                "readability": [
                    {
                        "strict": "yes",
                        "extraction_policy": "keep_existing"
                    },
                    {
                        "strict": "no",
                        "extraction_policy": "keep_existing",
                        "field_name": "content_relaxed"
                    }
                ]
            }
        }
        }
        c = Core(extraction_config=e_config)
        r = c.process(self.doc)
        self.assertTrue("content_extraction" in r)

        self.assertTrue("content_strict" in r["content_extraction"])
        self.assertTrue("content_relaxed" in r["content_extraction"])
        self.assertTrue("title" in r["content_extraction"])
        self.assertTrue("inferlink_extractions" in r["content_extraction"])

    def test_document_id_not_present(self):
        e_config = {
            'document_id': 'blah'
        }
        c = Core(extraction_config=e_config)
        with self.assertRaises(KeyError):
            r = c.process(self.doc)

    def test_document_id(self):
        e_config = {
            'document_id': 'doc_id'
        }
        c = Core(extraction_config=e_config)
        r = c.process(self.doc)
        self.assertTrue('document_id' in r)
        doc_id = '1A4A5FF5BD066309C72C8EEE6F7BCCCFD21B83245AFCDADDF014455BCF990A21'
        self.assertEqual(r['document_id'], doc_id)

    def test_json_content_path(self):
        e_config = {
            "extraction_policy": "replace",
            "error_handling": "raise_error",
            "document_id": "uri",
            "content_extraction": {
                "json_content": [
                    {
                        "input_path": "@graph[*].\"bioc:text\"",
                        "segment_name": "bioc_text"
                    },
                    {
                        "input_path": "@graph[*].random_field",
                        "segment_name": "random_field"
                    }
                ]
            },
            "data_extraction": [
                {
                    "input_path": "content_extraction.bioc_text[*].text.`parent`"
                    ,
                    "fields": {
                        "character": {
                            "extractors": {
                                "extract_as_is": {
                                    "extraction_policy": "keep_existing"
                                }
                            }

                        }
                    }
                },
                {
                    "input_path": "content_extraction.random_field[*].text.`parent`"
                    ,
                    "fields": {
                        "catch_phrase": {
                            "extractors": {
                                "extract_as_is": {
                                    "extraction_policy": "keep_existing"
                                }
                            }

                        }
                    }
                }
            ]
        }

        doc = {
            "uri": "1",
            "url": "http://itsagoodshow.com",
            "@graph": [
                {
                    "bioc:text": "Rick Sanchez",
                    "random_field": "wubba lubba dub dub"
                },
                {
                    "bioc:text": "Morty Smith",
                    "random_field": "aww jeez man"
                }
            ]
        }
        c = Core(extraction_config=e_config)
        r = c.process(doc, create_knowledge_graph=True)
        self.assertTrue("content_extraction" in r)
        self.assertTrue("bioc_text" in r["content_extraction"])
        t = r["content_extraction"]['bioc_text']
        self.assertTrue(len(t) == 2)
        self.assertTrue("knowledge_graph" in r)
        self.assertTrue("character" in r["knowledge_graph"])
        self.assertTrue("catch_phrase" in r["knowledge_graph"])
        expected_characters = ['rick sanchez', 'morty smith']
        expected_phrases = ['wubba lubba dub dub', 'aww jeez man']
        for c in r['knowledge_graph']['character']:
            self.assertTrue(c['key'] in expected_characters)

        for c in r['knowledge_graph']['catch_phrase']:
            self.assertTrue(c['key'] in expected_phrases)

    def test_black_extract_as_is(self):
        doc = {
            "uri": "1",
            "event_actors": [
                {
                    "description": "Non-State, Internal, No State Sanction",
                    "id": "internalnononstatesanctionstate",
                    "title": ""
                },
                {
                    "description": "Noncombatant Status Asserted",
                    "id": "assertedcontestednoncombatantnoncombatantnotstatusstatus",
                    "title": "Noncombatant Status Not Contested"
                }
            ]
        }

        e_config = {
            "extraction_policy": "replace",
            "error_handling": "raise_error",
            "document_id": "uri",
            "content_extraction": {
                "json_content": [
                    {
                        "input_path": "event_actors[*].title",
                        "segment_name": "actor_title"
                    }
                ]
            },
            "data_extraction": [
                {
                    "input_path": "content_extraction.actor_title[*].text.`parent`",
                    "fields": {
                        "actor_title": {
                            "extractors": {
                                "extract_as_is": {
                                    "extraction_policy": "keep_existing"
                                }
                            }

                        }
                    }
                }
            ]
        }
        c = Core(extraction_config=e_config)
        r = c.process(doc)

        self.assertTrue('actor_title' in r['knowledge_graph'])
        self.assertTrue(len(r['knowledge_graph']['actor_title']) == 1)
        self.assertTrue(r['knowledge_graph']['actor_title'][0]['key'] == 'noncombatant status not contested')

    def test_extract_as_is_post_filter(self):
        doc = {
            "uri": "1",
            "event_actors": [
                {
                    "description": "Non-State, Internal, No State Sanction",
                    "id": "internalnononstatesanctionstate",
                    "title": ""
                },
                {
                    "description": "Noncombatant Status Asserted",
                    "id": "assertedcontestednoncombatantnoncombatantnotstatusstatus",
                    "title": "Noncombatant Status Not Contested"
                }
            ]
        }

        e_config = {
            "extraction_policy": "replace",
            "error_handling": "raise_error",
            "document_id": "uri",
            "content_extraction": {
                "json_content": [
                    {
                        "input_path": "event_actors[*].title",
                        "segment_name": "actor_title"
                    }
                ]
            },
            "data_extraction": [
                {
                    "input_path": "content_extraction.actor_title[*].text.`parent`",
                    "fields": {
                        "actor_title": {
                            "extractors": {
                                "extract_as_is": {
                                    "extraction_policy": "keep_existing",
                                    "config": {
                                        "post_filter": [
                                            "x.upper()"
                                        ]
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
        self.assertTrue('actor_title' in r['knowledge_graph'])
        self.assertTrue(len(r['knowledge_graph']['actor_title']) == 1)
        self.assertTrue(r['knowledge_graph']['actor_title'][0]['value'] == 'noncombatant status not contested'.upper())

    def test_extract_as_is_post_filter_2(self):
        doc = {
            "uri": "1",
            "event_actors": [
                {
                    "description": "Non-State, Internal, No State Sanction",
                    "id": "internalnononstatesanctionstate",
                    "title": ""
                },
                {
                    "description": "Noncombatant Status Asserted",
                    "id": "assertedcontestednoncombatantnoncombatantnotstatusstatus",
                    "title": "Noncombatant Status Not Contested"
                }
            ]
        }

        e_config = {
            "extraction_policy": "replace",
            "error_handling": "raise_error",
            "document_id": "uri",
            "content_extraction": {
                "json_content": [
                    {
                        "input_path": "event_actors[*].title",
                        "segment_name": "actor_title"
                    }
                ]
            },
            "data_extraction": [
                {
                    "input_path": "content_extraction.actor_title[*].text.`parent`",
                    "fields": {
                        "actor_title": {
                            "extractors": {
                                "extract_as_is": {
                                    "extraction_policy": "keep_existing",
                                    "config": {
                                        "post_filter": [
                                            "isinstance(x, dict)"
                                        ]
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
        self.assertTrue('knowledge_graph' not in r)

    def test_extract_as_is_post_filter_3(self):
        doc = {
            "uri": "1",
            "event_actors": [
                {
                    "description": "Non-State, Internal, No State Sanction",
                    "id": "internalnononstatesanctionstate",
                    "size": "54"
                },
                {
                    "description": "Noncombatant Status Asserted",
                    "id": "assertedcontestednoncombatantnoncombatantnotstatusstatus",
                    "size": "red34"
                }
            ]
        }

        e_config = {
            "extraction_policy": "replace",
            "error_handling": "raise_error",
            "document_id": "uri",
            "content_extraction": {
                "json_content": [
                    {
                        "input_path": "event_actors[*].size",
                        "segment_name": "actor_size"
                    }
                ]
            },
            "data_extraction": [
                {
                    "input_path": "content_extraction.actor_size[*].text.`parent`",
                    "fields": {
                        "actor_size": {
                            "extractors": {
                                "extract_as_is": {
                                    "extraction_policy": "keep_existing",
                                    "config": {
                                        "post_filter": [
                                            "parse_number"
                                        ]
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
        self.assertEqual(r['knowledge_graph']['actor_size'][0]['value'], 54.0)

    def test_extract_as_is_artbitrary_path(self):
        doc = {
            "uri": "1",
            "event_actors": [
                {
                    "description": "Non-State, Internal, No State Sanction",
                    "id": "internalnononstatesanctionstate",
                    "title": ""
                },
                {
                    "description": "Noncombatant Status Asserted",
                    "id": "assertedcontestednoncombatantnoncombatantnotstatusstatus",
                    "title": "Noncombatant Status Not Contested"
                }
            ]
        }

        e_config = {
            "extraction_policy": "replace",
            "document_id": "uri",
            "data_extraction": [
                {
                    "input_path": "event_actors[*].description",
                    "fields": {
                        "actor_description": {
                            "extractors": {
                                "extract_as_is": {
                                }
                            }

                        }
                    }
                }
            ]
        }
        c = Core(extraction_config=e_config)
        r = c.process(doc)
        self.assertTrue('actor_description' in r['knowledge_graph'])
        self.assertTrue(len(r['knowledge_graph']['actor_description']) == 2)
        self.assertTrue(r['knowledge_graph']['actor_description'][0]['key'] in
                        ['noncombatant status not contested', 'non-state, internal, no state sanction'])

    def test_extract_as_is_data(self):
        doc = {
            "uri": "1",
            "event_actors": [
                {
                    "description": "Non-State, Internal, No State Sanction",
                    "id": "internalnononstatesanctionstate",
                    "title": ""
                },
                {
                    "description": "Noncombatant Status Asserted",
                    "id": "assertedcontestednoncombatantnoncombatantnotstatusstatus",
                    "title": "Noncombatant Status Not Contested"
                }
            ]
        }

        e_config = {
            "extraction_policy": "replace",
            "document_id": "uri",
            "data_extraction": [
                {
                    "input_path": "event_actors",
                    "fields": {
                        "actor_description": {
                            "extractors": {
                                "extract_as_is": {
                                }
                            }

                        }
                    }
                }
            ]
        }
        ex_data = [
                      {
                          "title": "",
                          "description": "Non-State, Internal, No State Sanction",
                          "id": "internalnononstatesanctionstate"
                      },
                      {
                          "title": "Noncombatant Status Not Contested",
                          "description": "Noncombatant Status Asserted",
                          "id": "assertedcontestednoncombatantnoncombatantnotstatusstatus"
                      }
                  ],
        c = Core(extraction_config=e_config)
        r = c.process(doc)
        self.assertTrue('actor_description' in r['knowledge_graph'])
        self.assertTrue(len(r['knowledge_graph']['actor_description']) == 1)
        self.assertTrue('key' in r['knowledge_graph']['actor_description'][0])
        self.assertEqual('AF856B829C1B8F798948076314052F8A833845C0C1C861BEE2C242C02BE6E7DA',
                         r['knowledge_graph']['actor_description'][0]['key'])
        self.assertTrue('value' in r['knowledge_graph']['actor_description'][0])
        self.assertTrue(len(r['knowledge_graph']['actor_description'][0]['data']) == 2)
        self.assertTrue(r['knowledge_graph']['actor_description'][0]['data'][0]['title'] in
                        ['', 'Noncombatant Status Not Contested'])
        self.assertTrue(r['knowledge_graph']['actor_description'][0]['data'][0]['description'] in
                        ['Non-State, Internal, No State Sanction', 'Noncombatant Status Asserted'])


if __name__ == '__main__':
    unittest.main()
