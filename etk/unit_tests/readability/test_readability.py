# -*- coding: utf-8 -*-
import os
import codecs
import unittest
import json
from structured_extractors import ReadabilityExtractor


class TestReadabilityExtractor(unittest.TestCase):

    def load_file(self, name):
        file = os.path.join(os.path.dirname(__file__), name)
        text = codecs.open(file, 'r', 'utf-8').read().replace('\n', '')
        return text

    def test_readability_extractor(self):
        dig_html = self.load_file("dig.html")
        dig_text = self.load_file("dig.txt")
        e = ReadabilityExtractor()
        options = {'recall_priority': True}
        readability_text = e.extract(dig_html, options)
        self.assertEquals(json.dumps(readability_text['text']), json.dumps(dig_text))


if __name__ == '__main__':
    unittest.main()
