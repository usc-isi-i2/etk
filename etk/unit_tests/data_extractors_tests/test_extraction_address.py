import unittest
import os, sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from data_extractors import address_extractor


class TestAddressExtractorMethods(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_address_extractor(self):
        text = 'Very passable black 25 year young TS girl with the best of the best! 9193959158 hosting off ' \
               'Western Boulevard NCstate area I\'m waiting! 20-40 $pecial$'

        extraction = address_extractor.extract(text)
        expected_extraction = [{'context': {'end': 110, 'start': 69}, 'value': 'hosting off western boulevard'}]

        self.assertEqual(extraction, expected_extraction)

    def test_multiple_address_extractor_with_context(self):
        text = 'The LA area has many airports.  LAX is located at 1 World Way, Los Angeles, CA 90045.  The Bob ' \
               'Hope airport is at 2627 N Hollywood Way, Burbank, CA 91505.  Both are very busy.'
        extraction = address_extractor.extract(text)
        expected_extraction = [{'context': {'start': 50, 'end': 62}, 'value': '1 world way,'},
                               {'context': {'start': 114, 'end': 135}, 'value': '2627 n hollywood way,'}]

        self.assertEqual(extraction, expected_extraction)


if __name__ == '__main__':
    unittest.main()