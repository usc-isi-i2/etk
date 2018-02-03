# coding: utf-8

import unittest
import sys
import os
import json

sys.path.append('../../')
sys.path.append('../')
from etk.core import Core


class TestExtractionsUsingSpacy(unittest.TestCase):
    def setUp(self):

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
                        },
                        "age": {
                            "extractors": {
                                "extract_using_spacy": {
                                    "config": {
                                    }
                                }
                            }
                        },
                        "social_media": {
                            "extractors": {
                                "extract_using_spacy": {
                                    "config": {
                                    }
                                }
                            }
                        },
                        "address": {
                            "extractors": {
                                "extract_using_spacy": {
                                    "config": {
                                    }
                                }
                            }
                        },
                        "email": {
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

        self.c = Core(extraction_config=e_config, load_spacy=True)
        self.ground_truth = dict()

        ground_truth_files = {"age": os.path.join(os.path.dirname(__file__), "ground_truth/age.jl"),
                              "date": os.path.join(os.path.dirname(__file__), "ground_truth/date.jl"),
                              "social_media": os.path.join(os.path.dirname(__file__), "ground_truth/social_media.jl"),
                              "address": os.path.join(os.path.dirname(__file__), "ground_truth/address.jl"),
                              "email": os.path.join(os.path.dirname(__file__), "ground_truth/email.jl")
                              }

        for extractor, file_name in ground_truth_files.items():
            with open(file_name, 'r') as f:
                test_data = f.read().split('\n')
                self.ground_truth[extractor] = list()
                for test_case in test_data:
                    self.ground_truth[extractor].append(json.loads(test_case))

    @staticmethod
    def create_list_from_kg(extractions):
        results = list()
        for e in extractions:
            ps = e['provenance']
            if not isinstance(ps, list):
                ps = [ps]
            for p in ps:
                results.append(p['extracted_value'])
        return results

    @staticmethod
    def create_list_from_social_media(extractions):
        results = dict()
        for e in extractions:
            ps = e['provenance']
            if not isinstance(ps, list):
                ps = [ps]
            for p in ps:
                x = p['qualifiers']['social_network']
                results[x] = [p['extracted_value']]
        return results

    def test_spacy_extractions(self):

        # Date extractor
        for t in self.ground_truth['date']:
            crf_tokens = self.c.extract_tokens_from_crf(
                self.c.extract_crftokens(t['text']))

            extraction_config = {'field_name': 'posting_date'}
            d = {'simple_tokens': crf_tokens}

            extracted_dates = self.c.extract_using_spacy(d, extraction_config)

            extracted_dates = [date['value'] for date in extracted_dates]

            correct_dates = t['extracted']

            self.assertEquals(extracted_dates, correct_dates)

        # Age extractor
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

        # Social media extractor
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

        # Address extractor
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

        # Extract using config

        # Date extractor
        for t in self.ground_truth['date']:
            r = self.c.process(t)
            if 'knowledge_graph' in r:
                extracted_dates = self.create_list_from_kg(r["knowledge_graph"]['posting_date'])
            else:
                extracted_dates = []

            correct_dates = t['extracted']

            self.assertEquals(extracted_dates, correct_dates)

        # Age extractor
        for t in self.ground_truth['age']:
            r = self.c.process(t)
            if 'knowledge_graph' in r:
                extracted_ages = self.create_list_from_kg(r["knowledge_graph"]['age'])
            else:
                extracted_ages = []

            self.assertEquals(sorted(extracted_ages), sorted(t['correct']))

        # Email extractor
        for t in self.ground_truth['email']:
            r = self.c.process(t)
            if 'knowledge_graph' in r:
                extracted_ages = self.create_list_from_kg(r["knowledge_graph"]['email'])
            else:
                extracted_ages = []
            self.assertEquals(sorted(extracted_ages), sorted(t['correct']))

        # Social media extractor
        for t in self.ground_truth['social_media']:
            for social_media in t['correct']:
                t['correct'][social_media] = [h.lower()
                                              for h in t['correct'][social_media]]
            extracted_social_media_handles = self.c.process(t)
            if 'knowledge_graph' in extracted_social_media_handles:
                extracted_social_media_handles = self.create_list_from_social_media(
                    extracted_social_media_handles["knowledge_graph"]['social_media'])
            else:
                extracted_social_media_handles = {}

            if len(extracted_social_media_handles) == 0 and len(t['correct']) == 0:
                self.assertFalse(extracted_social_media_handles)
            self.assertEquals(extracted_social_media_handles, t['correct'])

        # Address extractor
        for t in self.ground_truth['address']:
            r = self.c.process(t)
            if 'knowledge_graph' in r:
                extracted_addresses = self.create_list_from_kg(r["knowledge_graph"]['address'])
            else:
                extracted_addresses = []

            correct_addresses = t['extracted']
            self.assertEquals(extracted_addresses.sort(), correct_addresses.sort())

    def test_spacy_date(self):
        doc = {
            "url": "http://date.test.com",
            "doc_id": "12344",
            "content_extraction": {
                "useful_text": {
                    "text": u"Alert: Tue, 2006-02-07"
                }
            }
        }
        e_config = {
            "document_id": "doc_id",
            'data_extraction': [
                {
                    "fields": {
                        "event_date": {
                            "extractors": {
                                "extract_using_spacy": {
                                    "config": {
                                        "post_filter": "parse_date"
                                    }
                                }
                            }
                        }
                    },
                    "input_path": [
                        "content_extraction.useful_text.text.`parent`"
                    ]
                }
            ]}
        core = Core(extraction_config=e_config)
        r = core.process(doc)
        kg = r['knowledge_graph']
        self.assertTrue('event_date' in kg)
        self.assertEqual(kg['event_date'][0]['value'], '2006-02-07T00:00:00')


if __name__ == '__main__':
    unittest.main()
