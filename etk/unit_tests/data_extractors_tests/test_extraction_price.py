import unittest
import os, sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from data_extractors.digPriceExtractor import price_extractor

class TestPriceExtractorMethods(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_price_extractor(self):
        doc = {'content': 'Good morning I\'m doing incalls only gentleman I\'m quick 60 roses ?Hhr 80 roses '
                          '?Hour 120 roses unrushed and f.service provided nonnegotiable donations  614-563-3342'}
        extraction = price_extractor.extract(doc['content'])
        expected_extraction = [{'value': 60, 'metadata': {'currency': 'rose', 'time_unit': '30'}},
                               {'value': 80, 'metadata': {'currency': 'rose', 'time_unit': '60'}},
                               {'value': 120, 'metadata': {'currency': 'rose', 'time_unit': '60'}}]
        self.assertEqual(extraction, expected_extraction)

    def test_empty_price_extractor(self):
        doc = {'content': 'something unrelated'}

        extraction = price_extractor.extract(doc['content'])
        expected_extraction = []
        self.assertEqual(extraction, expected_extraction)


if __name__ == '__main__':
    unittest.main()