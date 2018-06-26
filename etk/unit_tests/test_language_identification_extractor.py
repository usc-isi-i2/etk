import unittest
from etk.extractors.language_identification_extractor import LanguageIdentificationExtractor


class TestLanguageIdentificationExtractor(unittest.TestCase):
    def test_langid(self):
        extractor = LanguageIdentificationExtractor()

        text_en = "langid.py comes pre-trained on 97 languages (ISO 639-1 codes given)"
        result_en = extractor.extract(text_en, "LANGID")
        self.assertEqual(result_en[0].value, "en")

        text_es = "Hola Mundo"
        result_es = extractor.extract(text_es, "LANGID")
        self.assertEqual(result_es[0].value, "es")

        text_de = "Ein, zwei, drei, vier"
        result_de = extractor.extract(text_de, "LANGID")
        self.assertEqual(result_de[0].value, "de")

        text_unknown = "%$@$%##"
        result_unknown = extractor.extract(text_unknown, "LANGID")
        self.assertEqual(result_unknown[0].value, "en")

    def test_langdetect(self):
        extractor = LanguageIdentificationExtractor()

        text_en = "langdetect supports 55 languages out of the box (ISO 639-1 codes)"
        result_en = extractor.extract(text_en, "LANGDETECT")
        self.assertEqual(result_en[0].value, "en")

        text_es = "Hola Mundo"
        result_es = extractor.extract(text_es, "LANGDETECT")
        self.assertEqual(result_es[0].value, "es")

        text_de = "Ein, zwei, drei, vier"
        result_de = extractor.extract(text_de, "LANGDETECT")
        self.assertEqual(result_de[0].value, "de")

        text_unknown = "%$@$%##"
        result_unknown = extractor.extract(text_unknown, "LANGDETECT")
        self.assertTrue(len(result_unknown) == 0)


if __name__ == '__main__':
    unittest.main()
