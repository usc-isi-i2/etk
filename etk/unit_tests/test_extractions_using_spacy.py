# coding: utf-8

import unittest
import sys
import os
import json
import gc
sys.path.append('../../')
sys.path.append('../')
from etk.core import Core
from spacy_extractors import age_extractor as spacy_age_extractor
from spacy_extractors import date_extractor as spacy_date_extractor
from spacy_extractors import social_media_extractor as spacy_social_media_extractor
from spacy_extractors import address_extractor as spacy_address_extractor


class TestExtractionsUsingSpacy(unittest.TestCase):

    def setUp(self):

        self.c = Core(load_spacy=True)
        self.ground_truth = dict()

        ground_truth_files = {"age": os.path.join(os.path.dirname(__file__), "ground_truth/age.jl"),
                              "date": os.path.join(os.path.dirname(__file__), "ground_truth/date.jl"),
                              "social_media": os.path.join(os.path.dirname(__file__), "ground_truth/social_media.jl"),
                              "address": os.path.join(os.path.dirname(__file__), "ground_truth/address.jl")
                              }

        for extractor, file_name in ground_truth_files.items():
            with open(file_name, 'r') as f:
                test_data = f.read().split('\n')
                self.ground_truth[extractor] = list()
                for test_case in test_data:
                    self.ground_truth[extractor].append(json.loads(test_case))

    def test_extraction_from_date_spacy(self):
        for t in self.ground_truth['date']:
            crf_tokens = self.c.extract_tokens_from_crf(
                self.c.extract_crftokens(t['text']))

            extraction_config = {'field_name': 'posting_date'}
            d = {'simple_tokens': crf_tokens}

            extracted_dates = self.c.extract_using_spacy(d, extraction_config)

            extracted_dates = [date['value'] for date in extracted_dates]

            correct_dates = t['extracted']

            self.assertEquals(extracted_dates, correct_dates)

    def test_extraction_from_age_spacy(self):
        for t in self.ground_truth['age']:

            crf_tokens = self.c.extract_tokens_from_crf(
                self.c.extract_crftokens(t['text']))

            extraction_config = {'field_name': 'age'}
            d = {'simple_tokens': crf_tokens}

            extracted_ages = self.c.extract_using_spacy(d, extraction_config)

            extracted_ages = [match['value'] for match in extracted_ages]

            if len(extracted_ages) == 0 and len(t['correct']) == 0:
                self.assertFalse(extracted_ages)

            self.assertEquals(sorted(extracted_ages), sorted(t['correct']))

    def test_extraction_from_social_media(self):
        for t in self.ground_truth['social_media']:

            for social_media in t['correct']:
                t['correct'][social_media] = [h.lower()
                                              for h in t['correct'][social_media]]

            crf_tokens = self.c.extract_tokens_from_crf(
                self.c.extract_crftokens(t['text']))

            extraction_config = {'field_name': 'social_media'}
            d = {'simple_tokens': crf_tokens}

            extracted_social_media_handles = self.c.extract_using_spacy(
                d, extraction_config)

            extracted_handles = dict()

            for match in extracted_social_media_handles:
                social_network = match['metadata']['social_network']
                if social_network not in extracted_handles:
                    extracted_handles[social_network] = [match['value']]
                else:
                    extracted_handles[social_network].append(match['value'])

            if len(extracted_social_media_handles) == 0 and len(t['correct']) == 0:
                self.assertFalse(extracted_social_media_handles)

            self.assertEquals(extracted_handles, t['correct'])

    def test_extraction_from_address_spacy(self):
        count = 1
        for t in self.ground_truth['address']:
            crf_tokens = self.c.extract_tokens_from_crf(
                self.c.extract_crftokens(t['text']))

            extraction_config = {'field_name': 'address'}
            d = {'simple_tokens': crf_tokens}

            extracted_addresses = self.c.extract_using_spacy(
                d, extraction_config)

            extracted_addresses = [address['value']
                                   for address in extracted_addresses]

            correct_addresses = t['extracted']

            self.assertEquals(extracted_addresses, correct_addresses)

        del self.c
        gc.collect()

    def test_extraction_from_date_spacy_using_config(self):
        e_config = {
            'data_extraction': [
                {
                    'input_path': 'text.`parent`',
                    'fields': {
                        "posting_date": {
                            "extractors": {
                                "extract_using_spacy": {
                                    "config": {
                                    }
                                }
                            }
                        }
                    }
                }
            ]}
        c = Core(extraction_config=e_config, load_spacy=True)

        for t in self.ground_truth['date']:
            r = c.process(t)
            if 'data_extraction' in r:
                extracted_dates = [x['value'] for x in r['data_extraction'][
                    'posting_date']['extract_using_spacy']['results']]
            else:
                extracted_dates = []

            correct_dates = t['extracted']

            self.assertEquals(extracted_dates, correct_dates)

        del c
        gc.collect()

    def test_extraction_from_age_spacy_using_config(self):
        e_config = {
            'data_extraction': [
                {
                    'input_path': 'text.`parent`',
                    'fields': {
                        "age": {
                            "extractors": {
                                "extract_using_spacy": {
                                    "config": {
                                    }
                                }
                            }
                        }
                    }
                }
            ]}
        c = Core(extraction_config=e_config, load_spacy=True)

        for t in self.ground_truth['age']:
            r = c.process(t)
            if 'data_extraction' in r:
                extracted_ages = [x['value'] for x in r['data_extraction'][
                    'age']['extract_using_spacy']['results']]
            else:
                extracted_ages = []

            self.assertEquals(sorted(extracted_ages), sorted(t['correct']))

        del c
        gc.collect()

    def test_extraction_from_social_media_spacy_using_config(self):
        e_config = {
            'data_extraction': [
                {
                    'input_path': 'text.`parent`',
                    'fields': {
                        "social_media": {
                            "extractors": {
                                "extract_using_spacy": {
                                    "config": {
                                    }
                                }
                            }
                        }
                    }
                }
            ]}
        c = Core(extraction_config=e_config, load_spacy=True)

        for t in self.ground_truth['social_media']:
            for social_media in t['correct']:
                t['correct'][social_media] = [h.lower()
                                              for h in t['correct'][social_media]]

            extracted_social_media_handles = c.process(t)

            if 'data_extraction' in extracted_social_media_handles:
                extracted_social_media_handles = [x for x in extracted_social_media_handles['data_extraction'][
                    'social_media']['extract_using_spacy']['results']]
            else:
                extracted_social_media_handles = []

            extracted_handles = dict()

            for match in extracted_social_media_handles:
                social_network = match['metadata']['social_network']
                if social_network not in extracted_handles:
                    extracted_handles[social_network] = [match['value']]
                else:
                    extracted_handles[social_network].append(match['value'])

            if len(extracted_social_media_handles) == 0 and len(t['correct']) == 0:
                self.assertFalse(extracted_social_media_handles)

            self.assertEquals(extracted_handles, t['correct'])

        del c
        gc.collect()

    def test_extraction_from_address_spacy_using_config(self):
        e_config = {
            'data_extraction': [
                {
                    'input_path': 'text.`parent`',
                    'fields': {
                        "address": {
                            "extractors": {
                                "extract_using_spacy": {
                                    "config": {
                                    }
                                }
                            }
                        }
                    }
                }
            ]}
        c = Core(extraction_config=e_config, load_spacy=True)

        for t in self.ground_truth['address']:
            r = c.process(t)
            if 'data_extraction' in r:
                extracted_addresses = [x['value'] for x in r['data_extraction'][
                    'address']['extract_using_spacy']['results']]
            else:
                extracted_addresses = []

            correct_addresses = t['extracted']

            self.assertEquals(extracted_addresses, correct_addresses)

        del c
        gc.collect()

if __name__ == '__main__':
    unittest.main()
