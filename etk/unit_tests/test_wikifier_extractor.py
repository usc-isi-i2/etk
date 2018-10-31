import unittest
from etk.extractors.wikifier_extractor import WikifierExtractor


class TestWikifierExtractor(unittest.TestCase):
    test_query = '''
        NASA JPL office located in Pasadena Los Angeles is losing funds as President Trump directs funds elsewhere.
    '''

    def test_wikifier_extractor(self) -> None:
        my_wikifier_extractor = WikifierExtractor()
        res = my_wikifier_extractor.extract(TestWikifierExtractor.test_query)
        self.assertEqual(len(res), 3)
        self.assertEqual(res[0].value['title'],"NASA")
        self.assertEqual(res[1].value['title'], "Pasadena, California")
        self.assertEqual(res[2].value['title'], "Los Angeles")


if __name__ == '__main__':
    unittest.main()
