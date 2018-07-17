import unittest
from etk.extraction import Extractable
from etk.tokenizer import Tokenizer


class TestExtractable(unittest.TestCase):

    def test_Extractable(self) -> None:
        e = Extractable({'extracted_value': [{1: 2, 'das': [1, 2, 3]}], 'confidence': 2.3})
        t = Tokenizer(keep_multi_space=False)
        tokens = e.get_tokens(t)
        token_attrs = []
        for i in tokens:
            token_attrs.append({"orth": i.orth_, "offset": i.idx, "full_shape": i._.full_shape})
        expected_token = [
            {'orth': 'extracted', 'offset': 0, 'full_shape': 'xxxxxxxxx'},
            {'orth': '_', 'offset': 9, 'full_shape': '_'},
            {'orth': 'value', 'offset': 10, 'full_shape': 'xxxxx'},
            {'orth': ':', 'offset': 16, 'full_shape': ':'},
            {'orth': '1', 'offset': 18, 'full_shape': 'd'},
            {'orth': ':', 'offset': 20, 'full_shape': ':'},
            {'orth': '2', 'offset': 22, 'full_shape': 'd'},
            {'orth': 'das', 'offset': 24, 'full_shape': 'xxx'},
            {'orth': ':', 'offset': 28, 'full_shape': ':'},
            {'orth': '1', 'offset': 30, 'full_shape': 'd'},
            {'orth': '2', 'offset': 32, 'full_shape': 'd'},
            {'orth': '3', 'offset': 34, 'full_shape': 'd'},
            {'orth': 'confidence', 'offset': 36, 'full_shape': 'xxxxxxxxxx'},
            {'orth': ':', 'offset': 47, 'full_shape': ':'},
            {'orth': '2.3', 'offset': 49, 'full_shape': 'd.d'}
        ]

        self.assertEqual(token_attrs, expected_token)
        text = e.get_string()
        expected_str = "extracted_value : 1 : 2 das : 1 2 3    confidence : 2.3 "

        self.assertEqual(text, expected_str)

if __name__ == '__main__':
    unittest.main()