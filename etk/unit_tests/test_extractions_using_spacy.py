from __future__ import unicode_literals
import unittest
import sys
import json
import time
sys.path.append('../../')
sys.path.append('../')
from etk.core import Core
from spacy_extractors import age_extractor as spacy_age_extractor
from spacy_extractors import date_extractor as spacy_date_extractor

class TestExtractionsUsingRegex(unittest.TestCase):

    def setUp(self):
        f = open('ground_truth/age.jl', 'r')

        data = f.read().split('\n')
        self.doc = []

        for t in data:
            self.doc.append(json.loads(t))

    '''
    def test_extractor(self):
        c = Core()

        start_time = time.time()
        # Load all the dictionaries here
        print '...loading spaCy'
        c.load_matchers()
        end_time = time.time()

        print "Time taken to load all the matchers: {0}".format(end_time - start_time)

        print "\nDate Extractor"
        date_docs = [
                    '23/05/2016',
                    '05/23/2016',
                    '23-05-2016',
                    '05-23-2016',
                    '23 May 2016',
                    '23rd May 2016',
                    '23rd May, 2016',
                    '23rd-05-2016',
                    'March 25, 2017',
                    'March 25th, 2017',
                    'March 25th 2017',
                    'March 25 2017',
                    'The meeting is on 23/05/2016',
                    'Can 05/23/2016 be the date of the meeting?',
                    'Lyonne was born on 23-05-2016 at 5 in the morning',
                    'Kramer is here on 05-23-2016',
                    'Google Inc is planning to make the acquisition on 23 May 2016',
                    'Romans and others will play this 23rd May 2016',
                    'Can 23rd May, 2016 be the day the Romans win?',
                ]

        for date_doc in date_docs:
            print date_doc
            print c.extract_date_spacy(date_doc)

        print "\nDate Extractor"
    '''

    def test_extraction_from_age_spacy(self):
        c = Core()
        for t in self.doc:
            extracted_ages = spacy_age_extractor.extract(t['content'], c.nlp, c.matchers['age'])
            extracted_ages = [age['value'] for age in extracted_ages]
            self.assertTrue(set(extracted_ages),set(t['correct']))

if __name__ == '__main__':
    unittest.main()
