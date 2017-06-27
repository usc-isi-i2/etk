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
        self.c = Core()
        self.ground_truth = dict()

        ground_truth_files = {"name": os.path.join(os.path.dirname(__file__), "ground_truth/name_my_name_1.jl"),
                              "name_i_am_2": os.path.join(os.path.dirname(__file__), "ground_truth/name_i_am_2.jl"),
                              "name_name_3": os.path.join(os.path.dirname(__file__), "ground_truth/name_name_3.jl"),
                              "name_it_is_4": os.path.join(os.path.dirname(__file__), "ground_truth/name_it_is_4.jl"),
                              "name_this_is_5": os.path.join(os.path.dirname(__file__), "ground_truth/name_this_is_5.jl"),
                              "name_im_6": os.path.join(os.path.dirname(__file__), "ground_truth/name_im_6.jl"),
                              "name_its_7": os.path.join(os.path.dirname(__file__), "ground_truth/name_its_7.jl"),
                              "name_teleph_number_split_8": os.path.join(os.path.dirname(__file__), "ground_truth/name_teleph_number_split_8.jl"),
                              "name_teleph_number_9": os.path.join(os.path.dirname(__file__), "ground_truth/name_teleph_number_9.jl")
                              }

        for extractor, file_name in ground_truth_files.items():
            with open(file_name, 'r') as f:
                test_data = f.read().split('\n')  #test_data is a list, contains dictionary
                self.ground_truth[extractor] = list()
                for test_case in test_data:
                    self.ground_truth[extractor].append(json.loads(test_case))  # ground_truth = {"text": 'Hello guy's, it's Jessica', 'extracted': 'Sara'}

    def generic_token(slef, type="word", token=[], shape=[], capitalization=[], part_of_speech=[], length=[],
                      prefix="", suffix="", is_followed_by_space="", is_required="true",
                      is_in_output="true", is_out_of_vocabulary="", is_in_vocabulary="", contain_digit=""):
        return {
            "type": type,
            "token": token,
            "shapes": shape,
            "capitalization": capitalization,
            "part_of_speech": part_of_speech,
            "length": length,
            "prefix": prefix,
            "suffix": suffix,
            "is_followed_by_space": is_followed_by_space,
            "is_required": is_required,
            "is_in_output": is_in_output,
            "is_out_of_vocabulary": is_out_of_vocabulary,
            "is_in_vocabulary": is_in_vocabulary,
            "contain_digit": contain_digit
        }

    def word_token(self, token=[], capitalization=[], part_of_speech=[], length=[], prefix="", suffix="",
                   is_followed_by_space="", is_required="true", is_in_output="false",
                   is_out_of_vocabulary="", is_in_vocabulary="", contain_digit=""):
        return self.generic_token(type="word", token=token, capitalization=capitalization,
                             part_of_speech=part_of_speech, length=length, prefix=prefix, suffix=suffix,
                             is_followed_by_space=is_followed_by_space, is_required=is_required,
                             is_in_output=is_in_output, is_out_of_vocabulary=is_out_of_vocabulary,
                             is_in_vocabulary=is_in_vocabulary, contain_digit=contain_digit)

    def punctuation_token(self, token=[], capitalization=[], part_of_speech=[], length=[], prefix="", suffix="",
                          is_followed_by_space="", is_required="true", is_in_output="false",
                          is_out_of_vocabulary="", is_in_vocabulary="", contain_digit=""):
        return self.generic_token(type="punctuation", token=token, capitalization=capitalization,
                             part_of_speech=part_of_speech, length=length, prefix=prefix, suffix=suffix,
                             is_followed_by_space=is_followed_by_space, is_required=is_required,
                             is_in_output=is_in_output, is_out_of_vocabulary=is_out_of_vocabulary,
                             is_in_vocabulary=is_in_vocabulary, contain_digit=contain_digit)

    def shape_token(self, shape=[], capitalization=[], part_of_speech=[], length=[], prefix="", suffix="",
                    is_followed_by_space="", is_required="true", is_in_output="false",
                    is_out_of_vocabulary="", is_in_vocabulary="", contain_digit=""):
        return self.generic_token(type="shape", shape=shape, capitalization=capitalization,
                             part_of_speech=part_of_speech, length=length, prefix=prefix, suffix=suffix,
                             is_followed_by_space=is_followed_by_space, is_required=is_required,
                             is_in_output=is_in_output, is_out_of_vocabulary=is_out_of_vocabulary,
                             is_in_vocabulary=is_in_vocabulary, contain_digit=contain_digit)


    #1.my name / names is
    def test_rule_my_name(self):
        field_rules = {
            "rules": [
                {
                    "identifier": "name_rule_01",
                    "description": "a description",
                    "is_active": "false",
                    "polarity": [],
                    "pattern": [
                        self.word_token(token=["my"]),
                        self.word_token(token=["name", "names"]),
                        self.word_token(token=["is"], is_required="false"),
                        self.word_token(capitalization=["title", "upper"], is_in_output="true")
                    ]
                }
            ]
        }

        for t in self.ground_truth['name']:
            crf_tokens = self.c.extract_tokens_from_crf(
                self.c.extract_crftokens(t['text'], lowercase=False))

            extraction_config = {'field_name': 'my_name_is'}
            d = {'simple_tokens_original_case': crf_tokens}

            extracted_names = self.c.extract_using_custom_spacy(d, extraction_config, field_rules= field_rules)

            extracted_names = [name['value'] for name in extracted_names]

            correct_names = t['extracted']

            self.assertEquals(extracted_names, correct_names)


#2. i am
    def test_rule_i_am(self):
        field_rules = {
            "rules": [
                {
                    "identifier": "name_rule_02",
                    "description": "a description",
                    "is_active": "true",
                    "polarity": [],
                    "pattern": [
                        self.word_token(token=["i"]),
                        self.word_token(token=["am"]),
                        self.word_token(capitalization=["title", "upper"], is_in_output="true")
                    ]
                }
            ]
        }

        for t in self.ground_truth['name_i_am_2']:
            crf_tokens = self.c.extract_tokens_from_crf(
                self.c.extract_crftokens(t['text'], lowercase=False))

            extraction_config = {'field_name': 'my_name_is'}
            d = {'simple_tokens_original_case': crf_tokens}

            extracted_names = self.c.extract_using_custom_spacy(d, extraction_config, field_rules=field_rules)

            extracted_names = [name['value'] for name in extracted_names]

            correct_names = t['extracted']

            self.assertEquals(extracted_names, correct_names)



#3. name :   Name:
    def test_rule_name_(self):
        field_rules = {
            "rules": [
                {
                    "identifier": "name_rule_03",
                    "description": "a description",
                    "is_active": "true",
                    "polarity": [],
                    "pattern": [
                        self.word_token(token=["name"]),
                        self.punctuation_token(token=[":"]),
                        self.word_token(token=[], is_in_output="true"),
                    ]
                }
            ]
        }

        for t in self.ground_truth['name_name_3']:
            crf_tokens = self.c.extract_tokens_from_crf(
                self.c.extract_crftokens(t['text'], lowercase=False))

            extraction_config = {'field_name': 'my_name_is'}
            d = {'simple_tokens_original_case': crf_tokens}

            extracted_names = self.c.extract_using_custom_spacy(d, extraction_config, field_rules= field_rules)

            extracted_names = [name['value'] for name in extracted_names]

            correct_names = t['extracted']

            self.assertEquals(extracted_names, correct_names)


# 4.it is
    def test_rule_it_is(self):
        field_rules = {
            "rules": [
                {
                    "identifier": "name_rule_04",
                    "description": "a description",
                    "is_active": "true",
                    "polarity": [],
                    "pattern": [
                        self.word_token(token=["it"]),
                        self.word_token(token=["is"]),
                        #           word_token(capitalization=["title", "mixed"], is_in_output="true")
                        self.word_token(part_of_speech=["proper noun"], is_in_output="true")
                    ]
                }
            ]
        }

        for t in self.ground_truth['name_it_is_4']:
            crf_tokens = self.c.extract_tokens_from_crf(
                self.c.extract_crftokens(t['text'], lowercase=False))

            extraction_config = {'field_name': 'my_name_is'}
            d = {'simple_tokens_original_case': crf_tokens}

            extracted_names = self.c.extract_using_custom_spacy(d, extraction_config, field_rules=field_rules)

            extracted_names = [name['value'] for name in extracted_names]

            correct_names = t['extracted']

            self.assertEquals(extracted_names, correct_names)



# 5.this is , This is
    def test_rule_this_is(self):
        field_rules = {
            "rules": [
                {
                    "identifier": "name_rule_05",
                    "description": "a description",
                    "is_active": "true",
                    "polarity": [],
                    "pattern": [
                        self.word_token(token=["this"]),
                        self.word_token(token=["is"]),
                        self.word_token(part_of_speech=["proper noun"], capitalization=["title", "mixed", "upper"],
                                   is_in_output="true")
                    ]
                }
            ]
        }

        for t in self.ground_truth['name_this_is_5']:
            crf_tokens = self.c.extract_tokens_from_crf(
                self.c.extract_crftokens(t['text'], lowercase=False))

            extraction_config = {'field_name': 'my_name_is'}
            d = {'simple_tokens_original_case': crf_tokens}

            extracted_names = self.c.extract_using_custom_spacy(d, extraction_config, field_rules=field_rules)

            extracted_names = [name['value'] for name in extracted_names]

            correct_names = t['extracted']

            self.assertEquals(extracted_names, correct_names)

# 6.I'm
    def test_rule_Im(self):
        field_rules = {
            "rules": [
                {
                    "identifier": "name_rule_06",
                    "description": "a description",
                    "is_active": "true",
                    "polarity": [],
                    "pattern": [
                        self.word_token(token=["i"]),
                        self.punctuation_token(token=["'"]),
                        self.word_token(token=["m"]),
                        self.word_token(part_of_speech=["proper noun"], capitalization=["title", "mixed", "upper"],
                                   is_in_output="true")
                    ]
                }
            ]
        }

        for t in self.ground_truth['name_im_6']:
            crf_tokens = self.c.extract_tokens_from_crf(
                self.c.extract_crftokens(t['text'], lowercase=False))

            extraction_config = {'field_name': 'my_name_is'}
            d = {'simple_tokens_original_case': crf_tokens}

            extracted_names = self.c.extract_using_custom_spacy(d, extraction_config, field_rules=field_rules)

            extracted_names = [name['value'] for name in extracted_names]

            correct_names = t['extracted']

            self.assertEquals(extracted_names, correct_names)


# 7. it's
    def test_rule_its(self):
        field_rules = {
            "rules": [
                {
                    "identifier": "name_rule_07",
                    "description": "a description",
                    "is_active": "true",
                    "polarity": [],
                    "pattern": [
                        self.word_token(token=["it"]),
                        self.punctuation_token(token=["'"]),
                        self.word_token(token=["s"]),
                        self.word_token(part_of_speech=["proper noun"], capitalization=["title", "mixed", "upper"],
                                   is_in_output="true")
                    ]
                }

            ]
        }

        for t in self.ground_truth['name_its_7']:
            crf_tokens = self.c.extract_tokens_from_crf(
                self.c.extract_crftokens(t['text'], lowercase=False))

            extraction_config = {'field_name': 'my_name_is'}
            d = {'simple_tokens_original_case': crf_tokens}

            extracted_names = self.c.extract_using_custom_spacy(d, extraction_config, field_rules=field_rules)

            extracted_names = [name['value'] for name in extracted_names]

            correct_names = t['extracted']

            self.assertEquals(extracted_names, correct_names)


# 8.Ashley (702)
    def test_rule_teleph_number_split(self):
        field_rules = {
            "rules": [
                {
                    "identifier": "name_rule_08",
                    "description": "a description",
                    "is_active": "true",
                    "polarity": [],
                    "pattern": [
                        self.word_token(capitalization=["title"], is_in_output="true"),
                        self.punctuation_token(token=["(", "["]),
                        self.shape_token(shape=["ddd"])
                    ]
                }
            ]
        }

        for t in self.ground_truth['name_teleph_number_split_8']:
            crf_tokens = self.c.extract_tokens_from_crf(
                self.c.extract_crftokens(t['text'], lowercase=False))

            extraction_config = {'field_name': 'my_name_is'}
            d = {'simple_tokens_original_case': crf_tokens}

            extracted_names = self.c.extract_using_custom_spacy(d, extraction_config, field_rules=field_rules)

            extracted_names = [name['value'] for name in extracted_names]

            correct_names = t['extracted']

            self.assertEquals(extracted_names, correct_names)


#9. Jessica 7135975313
    def test_rule_teleph_number(self):
        field_rules = {
            "rules": [
                {
                    "identifier": "name_rule_09",
                    "description": "a description",
                    "is_active": "true",
                    "polarity": [],
                    "pattern": [
                        self.word_token(capitalization=["title", "upper", "mixed"], is_in_output="true"),
                        self.shape_token(shape=["dddddddddd"])
                    ]
                }
            ]
        }

        for t in self.ground_truth['name_teleph_number_9']:
            crf_tokens = self.c.extract_tokens_from_crf(
                self.c.extract_crftokens(t['text'], lowercase=False))

            extraction_config = {'field_name': 'my_name_is'}
            d = {'simple_tokens_original_case': crf_tokens}

            extracted_names = self.c.extract_using_custom_spacy(d, extraction_config, field_rules=field_rules)

            extracted_names = [name['value'] for name in extracted_names]

            correct_names = t['extracted']

            self.assertEquals(extracted_names, correct_names)



if __name__ == '__main__':
    unittest.main()
