# -*- coding: utf-8 -*-
# @Author: ZwEin
# @Date:   2016-07-01 13:17:49
# @Last Modified by:   ZwEin
# @Last Modified time: 2016-10-02 15:10:53

import re

from unit import *


class ZEExtractor():

    re_digits = re.compile(r'\d+')
    re_alphabet = re.compile(r'[a-z]+')

    reg_time_units = r'(?:' + \
        r'(?:\d{1,3}[ ]?(?:' + r'|'.join(UNIT_TIME_HOUR + UNIT_TIME_MINUTE) + r'))' + r'|' \
        r'(?:' + r'|'.join(UNIT_TIME_UNITS) + r')' \
        r')'

    reg_separator = r'[\t ]?'
    reg_price_digit = r'\d{1,4}'
    reg_price_unit = r'(?:' + r'|'.join(UNIT_PRICE_UNITS) + r'){0,2}'  # \b
    reg_interval = r'\w{,30}'

    # patterns

    # pattern for # time
    reg_price_time = r'(?:' + \
        reg_price_unit + \
        reg_separator + \
        reg_price_digit + \
        reg_separator + \
        reg_price_unit + \
        reg_separator + \
        r'(?:' + reg_separator + r'for' + reg_separator + r')?' + \
        reg_separator + \
        reg_time_units + \
        ')'
    re_price_time = re.compile(reg_price_time)
    # r'(?:=(?:[a-z]+[\t ]){,5}?|[\t ]?)' + \
    # r'(?:' + reg_separator + r'for' + reg_separator + r')?' + \

    ## pattern for time #
    reg_time_price = r'(?:' + \
        reg_time_units + \
        reg_separator + \
        reg_price_unit + \
        reg_separator + \
        reg_price_digit + \
        reg_separator + \
        reg_price_unit + \
        ')'
    re_time_price = re.compile(reg_time_price)

    # pattern for price digits only
    reg_only_price = r'(?:' + \
        reg_price_unit + \
        reg_separator + \
        reg_price_digit + \
        reg_separator + \
        reg_price_unit + \
        ')'
    re_only_price = re.compile(reg_only_price)

    reg_combine = re.compile(
        r'(?:' + r'|'.join([reg_time_price, reg_price_time]) + r')')

    def filter(self, text_list):
        ans = []
        for text in text_list:
            if ZEExtractor.re_alphabet.findall(text):
                digits = ZEExtractor.re_digits.findall(text)
                if len(digits) == 1 and int(digits[0]) < 5:
                    continue
                ans.append(text)
        return ans

    def extract(self, text):
        text_pt_ext = ZEExtractor.re_price_time.findall(text)
        text_tp_ext = ZEExtractor.re_time_price.findall(text)
        text_op_ext = ZEExtractor.re_only_price.findall(text)

        if len(text_pt_ext) > len(text_tp_ext):
            target = text_pt_ext
        elif len(text_pt_ext) < len(text_tp_ext):
            target = text_tp_ext
        else:
            pool = [text_tp_ext, text_pt_ext, text_op_ext]
            pool_max_len = [sum([len(item) for item in _]) for _ in pool]
            target = pool[pool_max_len.index(max(pool_max_len))]

        extra = []
        target_digits = ZEExtractor.re_digits.findall(' '.join(target))

        for op_ext in text_op_ext:
            for opd in ZEExtractor.re_digits.findall(op_ext):
                if opd in target_digits:
                    break
                else:
                    extra.append(op_ext)
                    break
        target += extra

        return self.filter(target)

    def extract_from_list(self, text_list):
        extracted_text_list = [self.extract(
            cleaned_text) for cleaned_text in text_list]
        extracted_text = [val.strip()
                          for sublist in extracted_text_list for val in sublist]
        return extracted_text
