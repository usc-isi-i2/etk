# -*- coding: utf-8 -*-
# @Author: ZwEin
# @Date:   2016-06-09 13:43:42
# @Last Modified by:   ZwEin
# @Last Modified time: 2016-12-08 11:34:34

"""
tokenize original content formated in 'text' or 'url' separately, and removing punctuations

"""

import unicodedata
import sys
import string
import re

from urlparse import urlparse

SOURCE_TYPE_TEXT = 'text'
SOURCE_TYPE_URL = 'url'

punctuation_tbl = dict.fromkeys(i for i in xrange(sys.maxunicode)
                      if unicodedata.category(unichr(i)).startswith('P'))


class Tokenizer():

    re_2_digts_only_in_url_regex = re.compile(r'(?<=[-_])\d{2}(?=[_/])')
    re_all_alphabet_in_url_regex = re.compile(r'\w+')

    def __init__(self, source_type='text'):
        self.set_source_type(source_type)

    def set_source_type(self, source_type):
        """
        'text' or 'url'

        """
        if source_type.lower() not in [SOURCE_TYPE_TEXT, SOURCE_TYPE_URL]:
            raise Exception(
                source_type +
                ' is not a source type, which should be "text" or "url"')

        self.source_type = source_type

    def remove_punctuation(self, raw):
        if isinstance(raw, str):
            return raw.translate(string.maketrans("", ""), string.punctuation)
        elif isinstance(raw, unicode):
            return raw.translate(punctuation_tbl)
        else:
            return raw

    def tokenize(self, raw):
        result = None
        if self.source_type == SOURCE_TYPE_TEXT:
            result = self.tokenize_text(raw)
        elif self.source_type == SOURCE_TYPE_URL:
            result = self.tokenize_url(raw)
        return ' '.join(result.split())

    def tokenize_text(self, raw):
        tokens = raw
        tokens = ' '.join(tokens)
        tokens = self.remove_punctuation(tokens)
        return tokens

    def tokenize_url(self, raw):
        SEPARATOR = ' '

        url_obj = urlparse(raw)

        # parse netloc
        # get rid of port numbers, ext and domain name
        netloc = url_obj.netloc.split('.')[:-2]

        # parse path
        path = url_obj.path
        path = Tokenizer.re_2_digts_only_in_url_regex.sub('', path)
        path = path.split('/')

        content = netloc + path

        content = [SEPARATOR.join(
            Tokenizer.re_all_alphabet_in_url_regex.findall(_)) for _ in content]

        # parse params
        # url_obj.params

        # parse query
        # url_obj.query
        return ' sep '.join(content)
