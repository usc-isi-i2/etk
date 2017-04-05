#!/usr/bin/python

# -*- coding: utf-8 -*-
# @Author: ZwEin
# @Date:   2016-06-13 23:15:52
# @Last Modified by:   ZwEin
# @Last Modified time: 2016-12-08 11:39:48

"""
main file for phone number matcher

"""

from .core.preprocessor import Preprocessor
from .core.tokenizer import Tokenizer
from .core.extractor import Extractor
from .core.cleaner import Cleaner
from .core.validator import Validator
from .core.normalizer import Normalizer


class PhoneNumberMatcher():

    PN_OUTPUT_FORMAT_LIST = 'list'
    PN_OUTPUT_FORMAT_OBFUSCATION = 'obfuscation'

    def __init__(self, _output_format='list'):
        self.preprocessor = Preprocessor()
        self.tokenizer = Tokenizer(source_type='text')
        self.extractor = Extractor()
        self.cleaner = Cleaner()
        self.validator = Validator()
        self.normalizer = Normalizer()
        self.set_output_format(_output_format)

    def set_output_format(self, _output_format):
        # 1. list, 2. obfuscation
        if _output_format not in [PhoneNumberMatcher.PN_OUTPUT_FORMAT_LIST, PhoneNumberMatcher.PN_OUTPUT_FORMAT_OBFUSCATION]:
            raise Exception('output_format should be "list" or "obfuscation"')
        self.output_format = _output_format

    def do_process(self, content, source_type='text', do_preprocess=True,
                   do_tokenize=True, do_clean=True, do_extract=True,
                   do_validate=True, include_context=False):
        if do_tokenize:
            self.tokenizer.set_source_type(source_type)
            content = self.tokenizer.tokenize(content)

        if do_preprocess:
            content = self.preprocessor.preprocess(content)

        if do_clean:
            content = self.cleaner.clean(content)

        if do_extract:
            content = self.extractor.extract(content)

        if do_validate:
            content = self.validator.validate(content)

        return content

        
    def match(self, content, source_type='text', include_context=False):
        cleaned_ans = self.do_process(content, source_type=source_type)
        uncleaned_ans = self.do_process(content, source_type=source_type, do_clean=False)
        return self.normalizer.normalize(cleaned_ans, uncleaned_ans, output_format=self.output_format, include_context=include_context)





