import unittest
from etk_extraction import Extractable


class TestExtractable(unittest.TestCase):

    def test_Extractable(self):
        e = Extractable({'extracted_value': [{1: 2, 'das': [1, 2, 3]}], 'confidence': 2.3})
        tokens = e.get_tokens()
        token_attrs = []
        for i in tokens:
            token_attrs.append({"orth": i.orth_, "offset": i.idx, "full_shape": i._.full_shape})

        expected_token = [
            {'orth': 'extracted_value', 'offset': 0, 'full_shape': 'xxxxxxxxx_xxxxx'},
            {'orth': ':', 'offset': 16, 'full_shape': ':'},
            {'orth': '1', 'offset': 18, 'full_shape': 'd'},
            {'orth': ':', 'offset': 20, 'full_shape': ':'},
            {'orth': '2', 'offset': 22, 'full_shape': 'd'},
            {'orth': ' ', 'offset': 24, 'full_shape': ' '},
            {'orth': 'das', 'offset': 25, 'full_shape': 'xxx'},
            {'orth': ':', 'offset': 29, 'full_shape': ':'},
            {'orth': '1', 'offset': 31, 'full_shape': 'd'},
            {'orth': ' ', 'offset': 33, 'full_shape': ' '},
            {'orth': '2', 'offset': 34, 'full_shape': 'd'},
            {'orth': ' ', 'offset': 36, 'full_shape': ' '},
            {'orth': '3', 'offset': 37, 'full_shape': 'd'},
            {'orth': '       ', 'offset': 39, 'full_shape': '       '},
            {'orth': 'confidence', 'offset': 46, 'full_shape': 'xxxxxxxxxx'},
            {'orth': ':', 'offset': 57, 'full_shape': ':'},
            {'orth': '2.3', 'offset': 59, 'full_shape': 'd.d'},
            {'orth': ' ', 'offset': 63, 'full_shape': ' '}
        ]

        self.assertEqual(token_attrs, expected_token)

        expected_str = "extracted_value : 1 : 2  das : 1  2  3        confidence : 2.3  "
        text = e.get_string()
        self.assertEqual(text, expected_str)

if __name__ == '__main__':
    unittest.main()