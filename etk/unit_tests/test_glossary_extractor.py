import unittest
from etk.extractors.glossary_extractor import GlossaryExtractor
from etk.tokenizer import Tokenizer
from etk.etk import ETK
from etk.document import Document
import time


class TestGlossaryExtractor(unittest.TestCase):
    def setUp(self):
        self.glossary_1 = ['Beijing', 'Los Angeles', 'New York', 'Shanghai']
        self.glossary_2 = ['Beijing', 'Los Angeles', 'New York City', 'Shanghai']

    def test_glossary_extractor(self) -> None:
        t = Tokenizer()
        text = 'i live in los angeles. my hometown is Beijing. I love New York City.'
        tokens = t.tokenize(text)

        ge = GlossaryExtractor(self.glossary_1, 'test_glossary', t, 3, False)
        results = [i.value for i in ge.extract(tokens)]
        expected = ['Beijing', 'los angeles', 'New York']

        self.assertEqual(results, expected)

    def test_case_sensitive(self) -> None:
        t = Tokenizer()
        text = 'i live in los angeles. my hometown is Beijing. I love New York City.'
        tokens = t.tokenize(text)

        ge = GlossaryExtractor(self.glossary_1, 'test_glossary', t, 2, True)

        results = [i.value for i in ge.extract(tokens)]
        expected = ['Beijing', 'New York']

        self.assertEqual(results, expected)

    def test_n_grams(self) -> None:
        t = Tokenizer()
        text = 'i live in los angeles. my hometown is Beijing. I love New York City.'
        tokens = t.tokenize(text)

        ge = GlossaryExtractor(self.glossary_2, 'test_glossary', t, 2, False)

        results = [i.value for i in ge.extract(tokens)]
        expected = ['Beijing', 'los angeles']

        self.assertEqual(results, expected)

    def test_etk__spacy_glossary_extraction(self):
        etk = ETK(use_spacy_tokenizer=True)
        s = time.time()
        city_extractor = GlossaryExtractor(['los angeles', 'new york', 'angeles'], 'city_extractor',
                                           etk.default_tokenizer,
                                           case_sensitive=False, ngrams=3)
        doc_json = {'text': 'i live in los angeles. my hometown is Beijing. I love New York City.'}
        doc = Document(etk, cdr_document=doc_json, mime_type='json', url='', doc_id='1')
        t_segments = doc.select_segments("$.text")
        for t_segment in t_segments:
            extracted_cities = doc.extract(city_extractor, t_segment)
            for extracted_city in extracted_cities:
                self.assertTrue(extracted_city.value in ['los angeles', 'New York', 'angeles'])

    def test_etk_crf_glossary_extraction(self):
        etk = ETK(use_spacy_tokenizer=False)
        s = time.time()
        city_extractor = GlossaryExtractor(['los angeles', 'new york', 'angeles'], 'city_extractor',
                                           etk.default_tokenizer,
                                           case_sensitive=False, ngrams=3)
        doc_json = {'text': 'i live in los angeles. my hometown is Beijing. I love New York City.'}
        doc = Document(etk, cdr_document=doc_json, mime_type='json', url='', doc_id='1')
        t_segments = doc.select_segments("$.text")
        for t_segment in t_segments:
            extracted_cities = doc.extract(city_extractor, t_segment)
            for extracted_city in extracted_cities:
                self.assertTrue(extracted_city.value in ['los angeles', 'New York', 'angeles'])


if __name__ == '__main__':
    unittest.main()
