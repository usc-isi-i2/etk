import unittest
import os, sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from data_extractors.digReviewIDExtractor import review_id_extractor


class TestReviewIDExtractorMethods(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_review_id_extractor(self):
        print "data_extractors.review_id.review_id_extractor"
        text = 'Hey guys! I\'m Heidi!!! I am a bubbly, busty, blonde massage therapist and only provide the most ' \
               'sensual therapeutic experience! I love what I do and so will YOU!!! I am always learning new techniqes ' \
               'and helping other feel relaxed. Just send Me an email and lets meet!!!  I am reviewed! #263289 ' \
               '\nheidishandsheal@gmail.com'

        extraction = review_id_extractor.extract(text)
        expected_extraction = [{'value': '263289', 'metadata': {'site': 'others'}}]
        self.assertEqual(extraction, expected_extraction)


if __name__ == '__main__':
    unittest.main()