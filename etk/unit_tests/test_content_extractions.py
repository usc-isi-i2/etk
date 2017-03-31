# -*- coding: utf-8 -*-
import unittest
import sys
sys.path.append('../../')
from etk.core import Core
from etk import core
import json
import codecs


class TestExtractions(unittest.TestCase):

    def setUp(self):
        self.doc = json.load(codecs.open('ground_truth/1.jl'))

    def test_no_config(self):
        e_config = None
        c = Core(extraction_config=e_config)
        r = c.process(self.doc)
        self.assertTrue(r)
        self.assertTrue("content_extraction" not in r)

    def test_ce_no_inputpath(self):
        e_config = {'content_extraction': {}}
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
        self.assertTrue("content_extraction" in r)

        self.assertTrue("content_strict" in r["content_extraction"])
        self.assertTrue("content_relaxed" in r["content_extraction"])
        self.assertTrue("title" not in r["content_extraction"])
        self.assertTrue("inferlink_extractions" not in r["content_extraction"])

        c_s = """\n \n \n \n \n \n smoothlegs24  28 \n \n \n chrissy391  27 \n \n \n My name is Helena height 160cms weight 55 kilos  contact me at escort.here@gmail.com           jefferson ave         age: 23 HrumpMeNow  28 \n \n \n xxtradition  24 \n \n \n jumblyjumb  26 \n \n \n claudia77  26 \n \n \n gushinPuss  28 \n \n \n Littlexdit  25 \n \n \n PinkSweets2  28 \n \n \n withoutlimit  27 \n \n \n bothOfUs3  28 \n \n \n lovelylips  27 \n \n \n killerbod  27 \n \n \n Littlexdit  27 \n \n \n azneyes  23 \n \n \n \n \n \n Escort's Phone: \n \n \n323-452-2013  \n \n Escort's Location: \nLos Angeles, California  \n Escort's Age:   23   Date of Escort Post:   Jan 02nd 6:46am \n REVIEWS:   \n READ AND CREATE REVIEWS FOR THIS ESCORT   \n \n \n \n \n \nThere are  50  girls looking in  .\n VIEW GIRLS \n \nHey I'm luna 3234522013 Let's explore , embrace and indulge in your favorite fantasy  % independent. discreet no drama Firm Thighs and Sexy. My Soft skin & Tight Grip is exactly what you deserve Call or text   Fetish friendly   Fantasy friendly   Party friendly 140 Hr SPECIALS 3234522013. Call  323-452-2013 .  Me and my friends are on EZsex  soooo you can find us all on there if you want... skittlegirl \n   \n \n   \n \n   \n Call me on my cell at 323-452-2013. Date of ad: 2017-01-02 06:46:00 \n \n \n"""
        c_r = """\n \n \n \n \n \n \n smoothlegs24  28 \n \n \n chrissy391  27 \n \n \n My name is Helena height 160cms weight 55 kilos  contact me at escort.here@gmail.com           jefferson ave         age: 23 HrumpMeNow  28 \n \n \n xxtradition  24 \n \n \n jumblyjumb  26 \n \n \n claudia77  26 \n \n \n gushinPuss  28 \n \n \n Littlexdit  25 \n \n \n PinkSweets2  28 \n \n \n withoutlimit  27 \n \n \n bothOfUs3  28 \n \n \n lovelylips  27 \n \n \n killerbod  27 \n \n \n Littlexdit  27 \n \n \n azneyes  23 \n \n \n \n \n \n Escort's Phone: \n \n \n323-452-2013  \n \n Escort's Location: \nLos Angeles, California  \n Escort's Age:   23   Date of Escort Post:   Jan 02nd 6:46am \n REVIEWS:   \n READ AND CREATE REVIEWS FOR THIS ESCORT   \n \n \n \n \n \nThere are  50  girls looking in  .\n VIEW GIRLS \n \nHey I'm luna 3234522013 Let's explore , embrace and indulge in your favorite fantasy  % independent. discreet no drama Firm Thighs and Sexy. My Soft skin & Tight Grip is exactly what you deserve Call or text   Fetish friendly   Fantasy friendly   Party friendly 140 Hr SPECIALS 3234522013. Call  323-452-2013 .  Me and my friends are on EZsex  soooo you can find us all on there if you want... skittlegirl \n   \n \n   \n \n   \n Call me on my cell at 323-452-2013. Date of ad: 2017-01-02 06:46:00 \n \n \n \n"""
        self.assertEqual(json.dumps(r["content_extraction"]["content_strict"]["text"]), json.dumps(c_s))
        self.assertEqual(json.dumps(r["content_extraction"]["content_relaxed"]["text"]), json.dumps(c_r))

        # self.assertTrue("tokens" in r["content_extraction"]["content_strict"])
        # self.assertTrue("simple_tokens" in r["content_extraction"]["content_strict"])
        # self.assertTrue(len(r["content_extraction"]["content_strict"]["tokens"]) > 0)
        # self.assertTrue(len(r["content_extraction"]["content_strict"]["simple_tokens"]) > 0)
        #
        # self.assertTrue("tokens" in r["content_extraction"]["content_relaxed"])
        # self.assertTrue("simple_tokens" in r["content_extraction"]["content_relaxed"])
        # self.assertTrue(len(r["content_extraction"]["content_relaxed"]["tokens"]) > 0)
        # self.assertTrue(len(r["content_extraction"]["content_relaxed"]["simple_tokens"]) > 0)

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
        e_config = {"resources": {
            "landmark": [
                "resources/consolidated_rules.json"
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
                "text": "Hey I'm luna 3234522013 Let's explore , embrace and indulge in your favorite fantasy % independent. discreet no drama Firm Thighs and Sexy. My Soft skin & Tight Grip is exactly what you deserve Call or text Fetish friendly Fantasy friendly Party friendly 140 Hr SPECIALS 3234522013"
            },
            "inferlink_posting-date": {
                "text": "2017-01-02 06:46"
            }
        }

        self.assertEqual(r["content_extraction"]["inferlink_extractions"], ifl_extractions)

    def test_landmark_no_field_name(self):
        e_config = {"resources": {
            "landmark": [
                "resources/consolidated_rules.json"
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
                "text": "Hey I'm luna 3234522013 Let's explore , embrace and indulge in your favorite fantasy % independent. discreet no drama Firm Thighs and Sexy. My Soft skin & Tight Grip is exactly what you deserve Call or text Fetish friendly Fantasy friendly Party friendly 140 Hr SPECIALS 3234522013"
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
        e_config = {"resources": {
            "landmark": [
                "resources/consolidated_rules.json"
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

if __name__ == '__main__':
    unittest.main()
