import unittest
from etk.extractors.glossary_extractor import GlossaryExtractor
from etk.tokenizer import Tokenizer


class TestGlossaryExtractor(unittest.TestCase):

    def test_glossary_extractor(self) -> None:
        t = Tokenizer()
        g = ['New York', 'Shanghai', 'Los Angeles', 'Beijing']
        ge = GlossaryExtractor(g, 'test_glossary', t, 2, False)
        text = 'i live in los angeles. my hometown is Beijing'
        tokens = t.tokenize(text)
        test_result = [i.value for i in ge.extract(tokens)]
        expected = ["Beijing", "los angeles"]
        self.assertEqual(test_result, expected)

    def test_case_sensitive(self) -> None:
        t = Tokenizer()
        g = ['New York', 'Shanghai', 'Los Angeles', 'Beijing']
        ge = GlossaryExtractor(g, 'test_glossary', t, 2, True)
        text = 'i live in los angeles. my hometown is Beijing'
        tokens = t.tokenize(text)
        test_result = [i.value for i in ge.extract(tokens)]
        expected = ["Beijing"]
        self.assertEqual(test_result, expected)


if __name__ == '__main__':
    unittest.main()