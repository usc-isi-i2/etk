# -*- coding: utf-8 -*-
import unittest
import sys, os

sys.path.append('../../')
from etk.core import Core
import json
import codecs


def generic_token(type="word", token=list(), shape=list(), number=list(), capitalization=list(), part_of_speech=list(),
                  length=list(), minimum="", maximum="", prefix="", suffix="", is_followed_by_space="",
                  is_required="true", is_in_output="true", is_out_of_vocabulary="", is_in_vocabulary="",
                  contain_digit=""):
    return {
        "type": type,
        "token": token,
        "shapes": shape,
        "numbers": number,
        "capitalization": capitalization,
        "part_of_speech": part_of_speech,
        "length": length,
        "minimum": minimum,
        "maximum": maximum,
        "prefix": prefix,
        "suffix": suffix,
        "is_required": is_required,
        "is_in_output": is_in_output,
        "is_out_of_vocabulary": is_out_of_vocabulary,
        "is_in_vocabulary": is_in_vocabulary,
        "contain_digit": contain_digit
    }


def word_token(token=list(), capitalization=list(), part_of_speech=list(), length=list(), minimum="", maximum="",
               prefix="", suffix="",
               is_required="true", is_in_output="false", is_out_of_vocabulary="", is_in_vocabulary="",
               contain_digit=""):
    return generic_token(type="word", token=token, capitalization=capitalization, part_of_speech=part_of_speech,
                         length=length, minimum=minimum, maximum=maximum, prefix=prefix, suffix=suffix,
                         is_required=is_required, is_in_output=is_in_output, is_out_of_vocabulary=is_out_of_vocabulary,
                         is_in_vocabulary=is_in_vocabulary, contain_digit=contain_digit)


def punctuation_token(token=list(), capitalization=list(), part_of_speech=list(), length=list(), minimum="", maximum="",
                      prefix="",
                      suffix="", is_required="true", is_in_output="false", is_out_of_vocabulary="", is_in_vocabulary="",
                      contain_digit=""):
    return generic_token(type="punctuation", token=token, capitalization=capitalization, part_of_speech=part_of_speech,
                         length=length, minimum=minimum, maximum=maximum, prefix=prefix, suffix=suffix,
                         is_required=is_required, is_in_output=is_in_output, is_out_of_vocabulary=is_out_of_vocabulary,
                         is_in_vocabulary=is_in_vocabulary, contain_digit=contain_digit)


def shape_token(shape=list(), capitalization=list(), part_of_speech=list(), length=list(), minimum="", maximum="",
                prefix="", suffix="",
                is_required="true", is_in_output="false", is_out_of_vocabulary="", is_in_vocabulary="",
                contain_digit=""):
    return generic_token(type="shape", shape=shape, capitalization=capitalization, part_of_speech=part_of_speech,
                         length=length, minimum=minimum, maximum=maximum, prefix=prefix, suffix=suffix,
                         is_required=is_required, is_in_output=is_in_output, is_out_of_vocabulary=is_out_of_vocabulary,
                         is_in_vocabulary=is_in_vocabulary, contain_digit=contain_digit)


def number_token(number=list(), capitalization=list(), part_of_speech=list(), length=list(), minimum="", maximum="",
                 prefix="",
                 suffix="", is_required="true", is_in_output="false", is_out_of_vocabulary="", is_in_vocabulary="",
                 contain_digit=""):
    return generic_token(type="number", number=number, capitalization=capitalization, part_of_speech=part_of_speech,
                         length=length, minimum=minimum, maximum=maximum, prefix=prefix, suffix=suffix,
                         is_required=is_required, is_in_output=is_in_output, is_out_of_vocabulary=is_out_of_vocabulary,
                         is_in_vocabulary=is_in_vocabulary, contain_digit=contain_digit)


class TestCustomSpacyNameExtraction(unittest.TestCase):
    def setUp(self):
        self.c = Core()
        self.data = dict()
        rule_01 = {
            "identifier": "name_rule_01",
            "description": "my name/names is",
            "is_active": "true",
            "output_format": "{1}",
            "pattern": [
                word_token(token=["my"]),
                word_token(token=["name", "names"]),
                word_token(token=["is"], is_required="false"),
                word_token(part_of_speech=["proper noun"], capitalization=["title", "upper"], is_in_output="true")
            ]
        }

        rule_02 = {
            "identifier": "name_rule_02",
            "description": "i am",
            "is_active": "true",
            "output_format": "{1}",
            "pattern": [
                word_token(token=["i"]),
                word_token(token=["am"]),
                word_token(capitalization=["title", "upper"], is_in_output="true")
            ]
        }

        rule_03 = {
            "identifier": "name_rule_03",
            "description": "name : Sara",
            "is_active": "true",
            "output_format": "{1}",
            "pattern": [
                word_token(token=["name"]),
                punctuation_token(token=[":"]),
                word_token(token=[], is_in_output="true"),
            ]
        }

        rule_04 = {
            "identifier": "name_rule_04",
            "description": "it is Jessicala",
            "is_active": "true",
            "output_format": "{1}",
            "pattern": [
                word_token(token=["it"]),
                word_token(token=["is"]),
                word_token(part_of_speech=["proper noun"], capitalization=["title", "upper"], is_in_output="true")
            ]
        }

        rule_05 = {
            "identifier": "name_rule_05",
            "description": "this is",
            "is_active": "true",
            "output_format": "{1}",
            "pattern": [
                word_token(token=["this"]),
                word_token(token=["is"]),
                word_token(part_of_speech=["proper noun"], capitalization=["title", "upper"], is_in_output="true")
            ]
        }

        rule_06 = {
            "identifier": "name_rule_06",
            "description": "i'm",
            "is_active": "true",
            "output_format": "{1}",
            "pattern": [
                word_token(token=["i"]),
                punctuation_token(token=["'"]),
                word_token(token=["m"]),
                word_token(part_of_speech=["proper noun"], capitalization=["title", "upper"], is_in_output="true")
            ]
        }

        rule_07 = {
            "identifier": "name_rule_07",
            "description": "it's",
            "is_active": "true",
            "output_format": "{1}",
            "pattern": [
                word_token(token=["it"]),
                punctuation_token(token=["'"]),
                word_token(token=["s"]),
                word_token(part_of_speech=["proper noun"], capitalization=["title", "upper"], is_in_output="true")
            ]
        }

        rule_08 = {
            "identifier": "name_rule_08",
            "description": "name followed by telephone number[123]",
            "is_active": "true",
            "output_format": "{1}",
            "pattern": [
                word_token(capitalization=["title"], is_in_output="true"),
                punctuation_token(token=["(", "["]),
                shape_token(shape=["ddd"])
            ]
        }

        rule_09 = {
            "identifier": "name_rule_09",
            "description": "name followed by telephone number 7135975313",
            "is_active": "true",
            "output_format": "{1}",
            "pattern": [
                word_token(capitalization=["title", "upper"], is_in_output="true"),
                shape_token(shape=["dddddddddd"])
            ]
        }

        text_01 = u"Hi Gentlemen, My name is Ashley . my name Monica I am the one and, My names is Alanda"
        text_02 = u"I'm Ashley I'm bored i am Alison, I am Gimly"
        text_03 = u"Name : Sara . I am the one and, Name: JILL , Name:Jessie"
        text_04 = u"Hello guy's, it's Jessica here from the #@%%% Spa. I cant say the name on here, and it is Jessica, " \
                  u"and it is cold"
        text_05 = u"this is Legolas I'm bored This is Danaerys  This is AshleyC"
        text_06 = text_02
        text_07 = text_04
        text_08 = u"Ashley (702)628-9035 XOXO . Aslll (702) 628-9035 XOXO Alppp 7026289035"
        text_09 = text_08

        self.data['1'] = dict()
        self.data['1']['text'] = text_01
        self.data['1']['rules'] = {"rules": [rule_01]}

        self.data['2'] = dict()
        self.data['2']['text'] = text_02
        self.data['2']['rules'] = {"rules": [rule_02]}

        self.data['3'] = dict()
        self.data['3']['text'] = text_03
        self.data['3']['rules'] = {"rules": [rule_03]}

        self.data['4'] = dict()
        self.data['4']['text'] = text_04
        self.data['4']['rules'] = {"rules": [rule_04]}

        self.data['5'] = dict()
        self.data['5']['text'] = text_05
        self.data['5']['rules'] = {"rules": [rule_05]}

        self.data['6'] = dict()
        self.data['6']['text'] = text_06
        self.data['6']['rules'] = {"rules": [rule_06]}

        self.data['7'] = dict()
        self.data['7']['text'] = text_07
        self.data['7']['rules'] = {"rules": [rule_07]}

        self.data['8'] = dict()
        self.data['8']['text'] = text_08
        self.data['8']['rules'] = {"rules": [rule_08]}

        self.data['9'] = dict()
        self.data['9']['text'] = text_09
        self.data['9']['rules'] = {"rules": [rule_09]}

        self.expected_data = dict()
        self.expected_data['1'] = dict()
        self.expected_data['1']['length'] = 3
        self.expected_data['1']['results'] = ['Ashley', 'Alanda', 'Monica']

        self.expected_data['2'] = dict()
        self.expected_data['2']['length'] = 2
        self.expected_data['2']['results'] = ['Alison', 'Gimly']

        self.expected_data['3'] = dict()
        self.expected_data['3']['length'] = 3
        self.expected_data['3']['results'] = ['Sara', 'JILL', 'Jessie']

        self.expected_data['4'] = dict()
        self.expected_data['4']['length'] = 1
        self.expected_data['4']['results'] = ['Jessica']

        self.expected_data['5'] = dict()
        self.expected_data['5']['length'] = 2
        self.expected_data['5']['results'] = ['Legolas', 'Danaerys']

        self.expected_data['6'] = dict()
        self.expected_data['6']['length'] = 1
        self.expected_data['6']['results'] = ['Ashley']

        self.expected_data['7'] = dict()
        self.expected_data['7']['length'] = 1
        self.expected_data['7']['results'] = ['Jessica']

        self.expected_data['8'] = dict()
        self.expected_data['8']['length'] = 2
        self.expected_data['8']['results'] = ['Ashley', 'Aslll']

        self.expected_data['9'] = dict()
        self.expected_data['9']['length'] = 1
        self.expected_data['9']['results'] = ['Alppp']

    def test_rules(self):
        for key in self.data.keys():
            d = dict()
            d['text'] = self.data[key]['text']
            d['simple_tokens_original_case'] = self.c.extract_tokens_from_crf(
                self.c.extract_crftokens(d['text'], lowercase=False))
            config = dict()
            config['field_name'] = 'name'
            results = self.c.extract_using_custom_spacy(d, config, field_rules=self.data[key]['rules'])
            self.assertTrue(len(results) == self.expected_data[key]['length'])
            for r in results:
                self.assertTrue(r['value'] in self.expected_data[key]['results'])