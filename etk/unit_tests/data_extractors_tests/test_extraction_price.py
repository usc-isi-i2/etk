import unittest
import os, sys
import codecs
import json
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from data_extractors.digPriceExtractor import price_extractor

class TestPriceExtractorMethods(unittest.TestCase):

    def setUp(self):
        file_path = os.path.join(os.path.dirname(__file__), "../test_price_extractor/groundtruth_price.jl")
        f = codecs.open(file_path, "r", "utf-8")
        self.doc = list()
        for line in f:
            self.doc.append(json.loads(line))

    def tearDown(self):
        pass

    def test_price_extractor(self):
        text = 'Good morning I\'m doing incalls only gentleman I\'m quick 60 roses ?Hhr 80 roses ' \
                          '?Hour 120 roses 120 min unrushed and f.service provided nonnegotiable donations  614-563-3342'

        extraction = price_extractor.extract(text)
        expected_extraction = [{'value': 60, 'metadata': {'currency': 'rose', 'time_unit': '30'}},
                               {'value': 80, 'metadata': {'currency': 'rose', 'time_unit': '60'}},
                               {'value': 120, 'metadata': {'currency': 'rose', 'time_unit': '120'}}]
        self.assertEqual(extraction, expected_extraction)

    def test_empty_price_extractor(self):
        text = 'something unrelated'

        extraction = price_extractor.extract(text)
        expected_extraction = []
        self.assertEqual(extraction, expected_extraction)

    def test_price_extractor_groundtruth(self):
        # outfile = codecs.open("p_output.jl", "w", "utf-8")
        for i, line in enumerate(self.doc):
            text = line["text"]
            extraction = price_extractor.extract(text)
            # outfile.write(json.dumps(extraction) + '\n')
            expected_extraction = line["extracted_price"]
            self.assertEqual(extraction, expected_extraction)
        # outfile.close()


if __name__ == '__main__':
    unittest.main()