import unittest
from etk.extractors.glossary_extractor import GlossaryExtractor
from etk.tokenizer import Tokenizer


class TestGlossaryExtractor(unittest.TestCase):

    def test_glossary_extractor(self) -> None:
        t = Tokenizer()
        text = 'i live in los angeles. my hometown is Beijing. I love New York City.'
        tokens = t.tokenize(text)

        g = ['Beijing', 'Los Angeles', 'New York', 'Shanghai']
        ge = GlossaryExtractor(g, 'test_glossary', t, 3, False)

        results = [i.value for i in ge.extract(tokens)]
        expected = ['Beijing', 'los angeles', 'New York']

        self.assertEqual(results, expected)

    def test_case_sensitive(self) -> None:
        t = Tokenizer()
        text = 'i live in los angeles. my hometown is Beijing. I love New York City.'
        tokens = t.tokenize(text)

        g = ['Beijing', 'Los Angeles', 'New York', 'Shanghai']
        ge = GlossaryExtractor(g, 'test_glossary', t, 2, True)

        results = [i.value for i in ge.extract(tokens)]
        expected = ['Beijing', 'New York']

        self.assertEqual(results, expected)

    def test_n_grams(self) -> None:
        t = Tokenizer()
        text = 'i live in los angeles. my hometown is Beijing. I love New York City.'
        tokens = t.tokenize(text)

        g = ['Beijing', 'Los Angeles', 'New York City', 'Shanghai']
        ge = GlossaryExtractor(g, 'test_glossary', t, 2, False)

        results = [i.value for i in ge.extract(tokens)]
        expected = ['Beijing', 'los angeles']

        self.assertEqual(results, expected)


if __name__ == '__main__':
    unittest.main()