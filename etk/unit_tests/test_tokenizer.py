import unittest
from etk.tokenizer import Tokenizer
import re


class TestTokenizer(unittest.TestCase):

    def test_tokenizer(self) -> None:
        text = "dsa@isi.edu 32.4 -32.1 (123)-345-6789, #1  \n \n   "
        reconstruct_text = re.sub(' +', ' ', text)
        t = Tokenizer()
        t.keep_multi_space = False
        tokens = t.tokenize(text)
        token_attrs = []
        for i in tokens:
            token_attrs.append({"orth": i.orth_, "offset": i.idx, "full_shape": i._.full_shape})
        expected = [
            {'orth': 'dsa', 'offset': 0, 'full_shape': 'xxx'},
            {'orth': '@', 'offset': 3, 'full_shape': '@'},
            {'orth': 'isi', 'offset': 4, 'full_shape': 'xxx'},
            {'orth': '.', 'offset': 7, 'full_shape': '.'},
            {'orth': 'edu', 'offset': 8, 'full_shape': 'xxx'},
            {'orth': '32.4', 'offset': 12, 'full_shape': 'dd.d'},
            {'orth': '-', 'offset': 17, 'full_shape': '-'},
            {'orth': '32.1', 'offset': 18, 'full_shape': 'dd.d'},
            {'orth': '(', 'offset': 23, 'full_shape': '('},
            {'orth': '123', 'offset': 24, 'full_shape': 'ddd'},
            {'orth': ')', 'offset': 27, 'full_shape': ')'},
            {'orth': '-', 'offset': 28, 'full_shape': '-'},
            {'orth': '345', 'offset': 29, 'full_shape': 'ddd'},
            {'orth': '-', 'offset': 32, 'full_shape': '-'},
            {'orth': '6789', 'offset': 33, 'full_shape': 'dddd'},
            {'orth': ',', 'offset': 37, 'full_shape': ','},
            {'orth': '#', 'offset': 39, 'full_shape': '#'},
            {'orth': '1', 'offset': 40, 'full_shape': 'd'},
            {'orth': '\n ', 'offset': 42, 'full_shape': '\n '},
            {'orth': '\n ', 'offset': 44, 'full_shape': '\n '}
        ]

        self.assertEqual(token_attrs, expected)
        self.assertEqual(t.reconstruct_text(tokens), reconstruct_text)

if __name__ == '__main__':
    unittest.main()