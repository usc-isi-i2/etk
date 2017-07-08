# -*- coding: utf-8 -*-
import unittest
import sys, os

sys.path.append('../../')
from etk.core import Core
import json
import codecs


class TestExtractionsInputPaths(unittest.TestCase):
    def setUp(self):
        file_path = os.path.join(os.path.dirname(__file__), "ground_truth/1_content_extracted.jl")
        self.doc = json.load(codecs.open(file_path))

    def test_extraction_input_path(self):
        women_name_file_path = os.path.join(os.path.dirname(__file__), "resources/female-names.json.gz")
        e_config = {"document_id": "doc_id",
                    "resources": {
                        "dictionaries": {
                            "women_name": women_name_file_path
                        }
                    },
                    "data_extraction": [
                        {
                            "input_path": "*.*.text.`parent`"
                            ,
                            "fields": {
                                "name": {
                                    "extractors": {
                                        "extract_using_dictionary": {
                                            "config": {
                                                "dictionary": "women_name",
                                                "ngrams": 1,
                                                "joiner": " ",
                                                "pre_process": [
                                                    "x.lower()"
                                                ],
                                                "pre_filter": [
                                                    "x"
                                                ],
                                                "post_filter": [
                                                    "isinstance(x, basestring)"
                                                ]
                                            },
                                            "extraction_policy": "keep_existing"
                                        },
                                        "extract_using_regex": {
                                            "config": {
                                                "include_context": "true",
                                                "regex": "(?:my[\\s]+name[\\s]+is[\\s]+([-a-z0-9@$!]+))",
                                                "regex_options": [
                                                    "IGNORECASE"
                                                ],
                                                "pre_filter": [
                                                    "x.replace('\\n', '')",
                                                    "x.replace('\\r', '')"
                                                ]
                                            },
                                            "extraction_policy": "replace"
                                        }
                                    }

                                }
                            }
                        }
                    ]
                    }
        c = Core(extraction_config=e_config)
        r = c.process(self.doc, create_knowledge_graph=True)
        self.assertTrue('knowledge_graph' in r)
        kg = r['knowledge_graph']
        expected_kg = {'title': [{'confidence': 1000, 'provenance': [{'source': {'segment': 'html', 'document_id': u'1A4A5FF5BD066309C72C8EEE6F7BCCCFD21B83245AFCDADDF014455BCF990A21'}, 'method': 'rearrange_title'}], 'value': u'323-452-2013 ESCORT ALERT! - Luna The Hot Playmate (323) 452-2013 - 23', 'key': 'title'}], 'name': [{'confidence': 1000, 'provenance': [{'source': {'segment': 'content_relaxed', 'context': {'end': 11, 'tokens_left': [u'27', u'\n\n\n', u'my', u'name', u'is'], 'text': "27 \n\n\n my name is <etk 'attribute' = 'name'>helena</etk> height 160cms weight 55 kilos ", 'start': 10, 'input': 'tokens', 'tokens_right': [u'height', u'160cms', u'weight', u'55', u'kilos']}, 'document_id': u'1A4A5FF5BD066309C72C8EEE6F7BCCCFD21B83245AFCDADDF014455BCF990A21'}, 'confidence': {'extraction': 1.0}, 'method': 'extract_using_dictionary', 'extracted_value': u'helena'}, {'source': {'segment': 'content_relaxed', 'context': {'start': 58, 'end': 75, 'input': 'text', 'text': " 27 \n \n \n  <etk 'attribute' = 'name'>My name is Helena</etk>  height 16"}, 'document_id': u'1A4A5FF5BD066309C72C8EEE6F7BCCCFD21B83245AFCDADDF014455BCF990A21'}, 'confidence': {'extraction': 1.0}, 'method': 'extract_using_regex', 'extracted_value': u'helena'}, {'source': {'segment': 'content_strict', 'context': {'end': 11, 'tokens_left': [u'27', u'\n\n\n', u'my', u'name', u'is'], 'text': "27 \n\n\n my name is <etk 'attribute' = 'name'>helena</etk> height 160cms weight 55 kilos ", 'start': 10, 'input': 'tokens', 'tokens_right': [u'height', u'160cms', u'weight', u'55', u'kilos']}, 'document_id': u'1A4A5FF5BD066309C72C8EEE6F7BCCCFD21B83245AFCDADDF014455BCF990A21'}, 'confidence': {'extraction': 1.0}, 'method': 'extract_using_dictionary', 'extracted_value': u'helena'}, {'source': {'segment': 'content_strict', 'context': {'start': 56, 'end': 73, 'input': 'text', 'text': " 27 \n \n \n  <etk 'attribute' = 'name'>My name is Helena</etk>  height 16"}, 'document_id': u'1A4A5FF5BD066309C72C8EEE6F7BCCCFD21B83245AFCDADDF014455BCF990A21'}, 'confidence': {'extraction': 1.0}, 'method': 'extract_using_regex', 'extracted_value': u'helena'}], 'value': u'helena', 'key': 'helena'}, {'confidence': 1000, 'provenance': [{'source': {'segment': 'content_relaxed', 'context': {'end': 137, 'tokens_left': [u'\n\n', u'hey', u'i', u"'", u'm'], 'text': "\n\n hey i ' m <etk 'attribute' = 'name'>luna</etk> 3234522013 let ' s explore ", 'start': 136, 'input': 'tokens', 'tokens_right': [u'3234522013', u'let', u"'", u's', u'explore']}, 'document_id': u'1A4A5FF5BD066309C72C8EEE6F7BCCCFD21B83245AFCDADDF014455BCF990A21'}, 'confidence': {'extraction': 1.0}, 'method': 'extract_using_dictionary', 'extracted_value': u'luna'}, {'source': {'segment': 'content_strict', 'context': {'end': 137, 'tokens_left': [u'\n\n', u'hey', u'i', u"'", u'm'], 'text': "\n\n hey i ' m <etk 'attribute' = 'name'>luna</etk> 3234522013 let ' s explore ", 'start': 136, 'input': 'tokens', 'tokens_right': [u'3234522013', u'let', u"'", u's', u'explore']}, 'document_id': u'1A4A5FF5BD066309C72C8EEE6F7BCCCFD21B83245AFCDADDF014455BCF990A21'}, 'confidence': {'extraction': 1.0}, 'method': 'extract_using_dictionary', 'extracted_value': u'luna'}, {'source': {'segment': 'title', 'context': {'end': 10, 'tokens_left': [u'2013', u'escort', u'alert', u'!', u'-'], 'text': "2013 escort alert ! - <etk 'attribute' = 'name'>luna</etk> the hot playmate ( 323 ", 'start': 9, 'input': 'tokens', 'tokens_right': [u'the', u'hot', u'playmate', u'(', u'323']}, 'document_id': u'1A4A5FF5BD066309C72C8EEE6F7BCCCFD21B83245AFCDADDF014455BCF990A21'}, 'confidence': {'extraction': 1.0}, 'method': 'extract_using_dictionary', 'extracted_value': u'luna'}], 'value': u'luna', 'key': 'luna'}], 'description': [{'confidence': 1000, 'provenance': [{'source': {'segment': 'content_strict', 'document_id': u'1A4A5FF5BD066309C72C8EEE6F7BCCCFD21B83245AFCDADDF014455BCF990A21'}, 'method': 'rearrange_description'}], 'key': 'description', 'value': u"\n \n \n \n \n \n smoothlegs24  28 \n \n \n chrissy391  27 \n \n \n My name is Helena height 160cms weight 55 kilos  contact me at escort.here@gmail.com           jefferson ave         age: 23 HrumpMeNow  28 \n \n \n xxtradition  24 \n \n \n jumblyjumb  26 \n \n \n claudia77  26 \n \n \n gushinPuss  28 \n \n \n Littlexdit  25 \n \n \n PinkSweets2  28 \n \n \n withoutlimit  27 \n \n \n bothOfUs3  28 \n \n \n lovelylips  27 \n \n \n killerbod  27 \n \n \n Littlexdit  27 \n \n \n azneyes  23 \n \n \n \n \n \n Escort's Phone: \n \n \n323-452-2013  \n \n Escort's Location: \nLos Angeles, California  \n Escort's Age:   23   Date of Escort Post:   Jan 02nd 6:46am \n REVIEWS:   \n READ AND CREATE REVIEWS FOR THIS ESCORT   \n \n \n \n \n \nThere are  50  girls looking in  .\n VIEW GIRLS \n \nHey I'm luna 3234522013 Let's explore , embrace and indulge in your favorite fantasy  % independent. discreet no drama Firm Thighs and Sexy. My Soft skin & Tight Grip is exactly what you deserve Call or text   Fetish friendly   Fantasy friendly   Party friendly 140 Hr SPECIALS 3234522013.\xa0Call  323-452-2013 .  Me and my friends are on EZsex  soooo you can find us all on there if you want... skittlegirl \n \xa0\xa0\n \n \xa0\xa0\n \n \xa0\xa0\n Call me on my cell at 323-452-2013. Date of ad: 2017-01-02 06:46:00 \n \n \n"}]}
        self.assertEqual(kg, expected_kg)


if __name__ == '__main__':
    unittest.main()
