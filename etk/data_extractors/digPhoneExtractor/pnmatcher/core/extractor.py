# -*- coding: utf-8 -*-
# @Author: ZwEin
# @Date:   2016-06-13 23:15:52
# @Last Modified by:   ZwEin
# @Last Modified time: 2016-12-09 11:40:56

"""
extract digits that seem good

"""

import re

class Extractor():

    def __init__(self):
        pass

    prefix = r'(?:(?<=[\A\b\sa-zA-Z])|^)'
    postfix = r'(?:(?=[\Z\b\sa-zA-Z])|$)'

    phone_number_format_regex = [
        r'(?:' + prefix + r"\d{10,13}" + postfix + r')',
        r'(?:' + prefix + r"\d{9,10}" + postfix + r')',
        r'(?:' + prefix + r"\d{8}[ ]\d{3,4}" + postfix + r')',
        r'(?:' + prefix + r"\d{7}[ ]\d{3,4}" + postfix + r')',
        r'(?:' + prefix + r"\d{6}[ ]\d{4}" + postfix + r')',
        r'(?:' + prefix + r"\d{5}[ ]\d{6}" + postfix + r')',
        r'(?:' + prefix + r"\d{5}[ ]\d{4}[ ]\d{4}" + postfix + r')',
        r'(?:' + prefix + r"\d{5}[ ]\d{4}" + postfix + r')',
        r'(?:' + prefix + r"\d{5}[ ]\d{4}[ ]\d{2}[ ]\d{2}" + postfix + r')',
        r'(?:' + prefix + r"\d{5}[ ]\d{3}[ ]\d{2}[ ]\d{2}" + postfix + r')',
        r'(?:' + prefix + r"\d{4}[ ]\d{4}[ ]\d{2}" + postfix + r')',
        r'(?:' + prefix +
        r"\d{4}[ ]\d{2}[ ]\d{2}[ ]\d{2}[ ]\d{2}" + postfix + r')',
        r'(?:' + prefix + r"\d{4}[ ]\d{3}[ ]\d{3}" + postfix + r')',
        r'(?:' + prefix + r"\d{3}[ ]\d{7,8}" + postfix + r')',
        r'(?:' + prefix + r"\d{3}[ ]\d{4}[ ]\d{4}" + postfix + r')',
        r'(?:' + prefix + r"\d{3}[ ]\d{4}[ ]\d{3}" + postfix + r')',
        r'(?:' + prefix + r"\d{3}[ ]\d{3}[ ]\d{4}" + postfix + r')',
        r'(?:' + prefix + r"\d{3}[ ]\d{3}[ ]\d{3}[ ]\d{1}" + postfix + r')',
        r'(?:' + prefix +
        r"\d{3}[ ]\d{3}[ ]\d{2}[ ]\d{1}[ ]\d{1}" + postfix + r')',
        r'(?:' + prefix + r"\d{3}[ ]\d{3}[ ]\d{1}[ ]\d{3}" + postfix + r')',
        r'(?:' + prefix +
        r"\d{3}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{4}" + postfix + r')',
        r'(?:' + prefix + r"\d{2}[ ]\d{4}[ ]\d{4}" + postfix + r')',
        r'(?:' + prefix + r"\d{2}[ ]\d{3}[ ]\d{3}[ ]\d{2}[ ]\d{2}" + postfix + r')',
        r'(?:' + prefix + r"\d{2}[ ]\d{8}" + postfix + r')',
        # \d{2}[ ] ...
        r'(?:' + prefix + r"\d{1}[ ]\d{8}[ ]\d{1}" + postfix + r')',
        r'(?:' + prefix + r"\d{1}[ ]\d{3}[ ]\d{3}[ ]\d{3}" + postfix + r')',
        r'(?:' + prefix + \
        r"\d{2}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}" + postfix + r')',
        r'(?:' + prefix + \
        r"\d{1}[ ]\d{2}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}" + postfix + r')',
        r'(?:' + prefix + \
        r"\d{1}[ ]\d{1}[ ]\d{2}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}" + postfix + r')',
        r'(?:' + prefix + \
        r"\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{2}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}" + postfix + r')',
        r'(?:' + prefix + \
        r"\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{2}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}" + postfix + r')',
        r'(?:' + prefix + \
        r"\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{2}[ ]\d{1}[ ]\d{1}[ ]\d{1}" + postfix + r')',
        r'(?:' + prefix + \
        r"\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{2}[ ]\d{1}[ ]\d{1}" + postfix + r')',
        r'(?:' + prefix + \
        r"\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{2}[ ]\d{1}" + postfix + r')',
        r'(?:' + prefix + \
        r"\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{2}" + postfix + r')',
        r'(?:' + prefix + \
        r"\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}" + postfix + r')'
    ]

    numbers_regex = r"(?:" + r"|".join(phone_number_format_regex) + r")"
    re_numbers_regex = re.compile(numbers_regex)

    def extract(self, raw):
        raw = Extractor.re_numbers_regex.findall(raw)
        raw = [''.join(_.split()) for _ in raw if len(_.strip()) >= 10] # .lstrip('0') if necessary
        return '\t'.join(raw)

if __name__ == '__main__':
    text = 'i am tall 5 feet 8 inches and curvy 36 c 28 38 587 645 7772'
    print Extractor().extract(text)
