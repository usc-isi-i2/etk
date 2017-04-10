import unittest
import sys
sys.path.append('../../')
print sys.path
from structured_extractors import TokenizerExtractor


class TestTokenizerExtractor(unittest.TestCase):

    def test_tokenizer_extractor(self):
        text = "my name is foo"
        t = TokenizerExtractor(recognize_linebreaks=True, create_structured_tokens=True)
        updated_doc = t.extract(text)
        expected = [{'char_end': 4, 'char_start': 2, 'type': 'normal', 'value': 'my'},
                    {'char_end': 11, 'char_start': 7, 'type': 'normal', 'value': 'name'},
                    {'char_end': 12, 'char_start': 10, 'type': 'normal', 'value': 'is'},
                    {'char_end': 16, 'char_start': 13, 'type': 'normal', 'value': 'foo'}]
        self.assertEqual(updated_doc, expected)

if __name__ == '__main__':
    unittest.main()
