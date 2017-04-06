import unittest
import sys
import json
import re
sys.path.append('../../')
sys.path.append('../')
from etk.core import Core
from spacy_extractors import age_extractor as spacy_age_extractor
from spacy_extractors import date_extractor as spacy_date_extractor


class TestExtractionsUsingRegex(unittest.TestCase):

    def setUp(self):
        
        self.c = Core()

        f = open('ground_truth/age.jl', 'r')

        data = f.read().split('\n')
        self.doc = {}
        self.doc['age'] = []

        for t in data:
            self.doc['age'].append(json.loads(t))

        f.close()

        f = open('ground_truth/date.jl', 'r')

        # data = f.read().split('\n')
        self.doc['date'] = []

        for t in f:
            self.doc['date'].append(json.loads(t))

        f.close()
    '''
    def test_extraction_from_date_spacy(self):

        for t in self.doc['date']:
            crf_tokens = self.c.extract_tokens_from_crf(
                self.c.extract_crftokens(t['content']))
            extracted_dates = spacy_date_extractor.extract(
                self.c.nlp, self.c.matchers['date'], crf_tokens)

            extracted_dates = [date['value'] for date in extracted_dates]

            correct_dates = [' '.join(self.c.extract_tokens_from_crf(self.c.extract_crftokens(
                re.sub(r'(\d)(st|nd|rd|th)', r'\1', x)))) for x in
                t['correct'].lower()]

            # print extracted_dates
            self.assertTrue(set(extracted_dates), set(correct_dates))

            break
    '''
    def test_extraction_from_age_spacy(self):
        
        for t in self.doc['age']:
            extracted_ages = spacy_age_extractor.extract(
                t['content'], self.c.nlp, self.c.matchers['age'])
            extracted_ages = [age['value'] for age in extracted_ages]
            self.assertEquals(set(extracted_ages), set(t['correct']))

if __name__ == '__main__':
    unittest.main()
