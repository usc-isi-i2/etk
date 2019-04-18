import unittest
from etk.extractors.sentence_extractor import SentenceExtractor

import spacy


class TestSentenceExtractor(unittest.TestCase):

    def setUp(self):
        self.sentence_extractor = SentenceExtractor()

    # Test1: simple split
    def test_simple_split(self):

        input_text = "Spacy's parser splits sentences better if the inputs " \
                     "are formatted with proper grammar. However, this is " \
                     "not always the case for text scraped from the internet."

        output_expected = ["Spacy's parser splits sentences better if the "
                           "inputs are formatted with proper grammar.",
                           "However, this is not always the case for text "
                           "scraped from the internet."]

        output_extractions = self.sentence_extractor.extract(input_text)
        for i, extraction in enumerate(output_extractions):
            self.assertEqual(extraction.value, output_expected[i])

    # Test2: ...end of sentence. start without caps
    def test_starts_lower(self):

        input_text = "Spacy's parser splits sentences better if the inputs " \
                     "are formatted with proper grammar. however, this is " \
                     "not always the case for text scraped from the internet."

        output_expected = ["Spacy's parser splits sentences better if the "
                           "inputs are formatted with proper grammar.",
                           "however, this is not always the case for text "
                           "scraped from the internet."]

        output_extractions = self.sentence_extractor.extract(input_text)
        for i, extraction in enumerate(output_extractions):
            self.assertEqual(extraction.value, output_expected[i])

    # Test3: ...end of sentence.Start without space
    def test_no_space(self):

        input_text = "Spacy's parser splits sentences better if the inputs " \
                     "are formatted with proper grammar.However, this is " \
                     "not always the case for text scraped from the internet."

        output_expected = ["Spacy's parser splits sentences better if the "
                           "inputs are formatted with proper grammar.",
                           "However, this is not always the case for text "
                           "scraped from the internet."]

        output_extractions = self.sentence_extractor.extract(input_text)
        for i, extraction in enumerate(output_extractions):
            self.assertEqual(extraction.value, output_expected[i])

    # Test4: ...end of sentence.worst case scenario
    def test_worst_case_scenario(self):

        input_text = "Spacy's parser splits sentences better if the inputs " \
                     "are formatted with proper grammar. However, this is " \
                     "not always the case for text scraped from the internet."

        output_expected = ["Spacy's parser splits sentences better if the "
                           "inputs are formatted with proper grammar.",
                           "However, this is not always the case for text "
                           "scraped from the internet."]

        output_desired = ["Spacy's parser splits sentences better if the "
                          "inputs are formatted with proper grammar.",
                          "however, this is not always the case for text "
                          "scraped from the internet."]

        output_extractions = self.sentence_extractor.extract(input_text)

        for i, extraction in enumerate(output_extractions):
            self.assertEqual(extraction.value, output_expected[i])

        with self.assertRaises(AssertionError):
            for i, extraction in enumerate(output_extractions):
                self.assertEqual(extraction.value, output_desired[i])

    # Test5: load custom_nlp -> run 1-4
    def test_ze_custom_nlp(self):

        my_nlp = spacy.load("en_core_web_sm")
        self.sentence_extractor = SentenceExtractor(custom_nlp=my_nlp)

        self.test_simple_split()
        self.test_starts_lower()
        self.test_no_space()
        self.test_worst_case_scenario()


if __name__ == "__main__":
    unittest.main()
