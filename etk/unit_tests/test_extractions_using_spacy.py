# coding: utf-8

import unittest
import sys
import os
import json
sys.path.append('../../')
sys.path.append('../')
from etk.core import Core
from spacy_extractors import age_extractor as spacy_age_extractor
from spacy_extractors import date_extractor as spacy_date_extractor
from spacy_extractors import social_media_extractor as spacy_social_media_extractor


class TestExtractionsUsingRegex(unittest.TestCase):

    def setUp(self):

        self.c = Core(load_spacy=True)
        self.ground_truth = dict()

        ground_truth_files = {"age": os.path.join(os.path.dirname(__file__), "ground_truth/age.jl"),
                              "date": os.path.join(os.path.dirname(__file__), "ground_truth/date.jl"),
                              "social_media": os.path.join(os.path.dirname(__file__), "ground_truth/social_media.jl")
                              }

        for extractor, file_name in ground_truth_files.items():
            with open(file_name, 'r') as f:
                test_data = f.read().split('\n')
                self.ground_truth[extractor] = list()
                for test_case in test_data:
                    self.ground_truth[extractor].append(json.loads(test_case))

        spacy_tokenizer = self.c.nlp.tokenizer
        self.c.nlp.tokenizer = lambda tokens: spacy_tokenizer.tokens_from_list(
            tokens)

    def test_extraction_from_date_spacy(self):
        for t in self.ground_truth['date']:
            crf_tokens = self.c.extract_tokens_from_crf(
                self.c.extract_crftokens(t['content']))
            nlp_doc = self.c.nlp(crf_tokens)

            extracted_dates = spacy_date_extractor.extract(
                nlp_doc, self.c.matchers['date'])

            extracted_dates = [date['value'] for date in extracted_dates]

            correct_dates = t['extracted']

            self.assertEquals(extracted_dates, correct_dates)

    def test_extraction_from_age_spacy(self):
        for t in self.ground_truth['age']:

            crf_tokens = self.c.extract_tokens_from_crf(
                self.c.extract_crftokens(t['content']))

            nlp_doc = self.c.nlp(crf_tokens)

            extracted_ages = spacy_age_extractor.extract(
                nlp_doc, self.c.matchers['age'])

            extracted_ages = [match['value'] for match in extracted_ages]
            if len(extracted_ages) == 0 and len(t['correct']) == 0:
                self.assertFalse(extracted_ages)

            self.assertEquals(sorted(extracted_ages), sorted(t['correct']))

    def test_extraction_from_social_media(self):
        for t in self.ground_truth['social_media']:

            crf_tokens = self.c.extract_tokens_from_crf(
                self.c.extract_crftokens(t['content']))

            nlp_doc = self.c.nlp(crf_tokens)

            extracted_social_media_handles = spacy_social_media_extractor.extract(
                nlp_doc, self.c.matchers['social_media'])

            #print extracted_social_media_handles

if __name__ == '__main__':
    unittest.main()
