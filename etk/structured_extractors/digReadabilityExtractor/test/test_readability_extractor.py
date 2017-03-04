import os
import codecs
import unittest
from readability_extractor import ReadabilityExtractor


class TestReadabilityExtractor(unittest.TestCase):

    def load_file(self, name):
        file = os.path.join(os.path.dirname(__file__), name)
        text = codecs.open(file, 'r', 'utf-8').read().replace('\n', '')
        return text

    def test_readability_extractor(self):
        dig_html = self.load_file("dig.html")
        dig_text = self.load_file("dig.txt")
        e = ReadabilityExtractor()
        print dig_html
        value = e.extract(dig_html)
        # self.assertEquals(updated_doc['extracted'][0]['result']['value'],dig_text)


if __name__ == '__main__':
    unittest.main()
