import unittest
import os, sys
import json
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from data_extractors.digPhoneExtractor import phone_extractor


class TestPhoneExtractorMethods(unittest.TestCase):


    def setUp(self):
        self.doc = {
            'content': [u'Sexy', u'new', u'girl', u'in', 'town', 'searching', 'for', 'a', 'great', 'date', 'wiff', 'u',
                        'Naughty', 'fresh', 'girl', 'here', 'searching', '4', 'a', 'great', 'date', 'wiff', 'you',
                        'Sweet', 'new', 'girl', 'in', 'town', 'seeking', 'for', 'a', 'good', 'date', 'with', 'u',
                        'for80', '2sixseven', 'one9zerofor', '90hr', 'incall', 'or', 'out', 'call'],
            'url': u'http://liveescortreviews.com/ad/philadelphia/602-228-4192/1/310054', 'b': 'world'}

    def tearDown(self):
        pass

    def test_phone_extractor_text(self):
        print "data_extractors.phone.phone_extractor_text"
        extraction = phone_extractor.extract(self.doc['content'], 'text', True, 'obfuscation')
        expected_extraction = [{'obfuscation': 'True', 'value': '4802671904'}]
        self.assertEqual(extraction, expected_extraction)

    def test_phone_extractor_url(self):
        print "data_extractors.phone.phone_extractor_url"
        extraction = phone_extractor.extract(self.doc['url'], 'url', True, 'obfuscation')
        expected_extraction = [{'obfuscation': 'False', 'value': '6022284192'}]
        self.assertEqual(extraction, expected_extraction)

    def test_phone_extractor_empty_tokens(self):
        print "data_extractors.phone.phone_extractor_empty_tokens"
        extraction = phone_extractor.extract([], 'text', True, 'obfuscation')
        expected_extraction = []
        self.assertEqual(extraction, expected_extraction)

if __name__ == '__main__':
    unittest.main()