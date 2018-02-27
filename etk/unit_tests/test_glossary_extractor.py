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
        expected = [
            {
              "extracted_value": "Beijing",
              "confidence": 1.0,
              "context": {
                "start": 9,
                "end": 10
              }
            },
            {
              "extracted_value": "Los Angeles",
              "confidence": 1.0,
              "context": {
                "start": 3,
                "end": 5
              }
            }
        ]
        self.assertEqual(test_result, expected)


if __name__ == '__main__':
    unittest.main()