import unittest
import re
from digExtractor.extractor import Extractor
from digExtractor.extractor_processor import ExtractorProcessor
from digTokenizerExtractor.tokenizer_extractor import TokenizerExtractor

class TestTokenizerExtractor(unittest.TestCase):


    def test_tokenizer_extractor(self):
        doc = { 'a': 'my name is foo', 'b': 'world'}
        e = TokenizerExtractor()
        ep = ExtractorProcessor().set_input_fields('a').set_output_field('e').set_extractor(e)
        updated_doc = ep.extract(doc)
        self.assertEqual(updated_doc['e'][0]['result'][0]['value'], ['my', 'name', 'is', 'foo'])

if __name__ == '__main__':
    unittest.main()
