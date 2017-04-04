# -*- coding: utf-8 -*-
# @Author: ZwEin
# @Date:   2016-06-14 16:17:20
# @Last Modified by:   ZwEin
# @Last Modified time: 2016-10-30 16:45:05

"""
preprocess digits that must not belong to phone number

"""
import re
from pnmatcher.core.common import datetime_helper


class Preprocessor():

    re_prep = re.compile(r'[\(\)]')

    reg_simple_format = [
        r'(?:\(\d{3}\)[ ]+\d{3}\-\d{4})',
        r'(?:(?<=[ \A\b-\.\?])\d{3}[ \?\.-]\d{3}[ \?\.-]\d{4}(?=[ \Z\b-\.\?]))'
    ]
    re_simple_format = re.compile(r'(?:' + r'|'.join(reg_simple_format) + r')')

    datetime_regexes = [
        r"(?:\d{2}[ _-]\d{2}[ _-]\d{4})",
        r"(?:\d{4}[ _-]\d{2}[ _-]\d{2})"
    ]
    datetime_regex = r"(?:" + r"|".join(datetime_regexes) + ")"

    re_datetime_regex = re.compile(datetime_regex)
    re_digits_regex = re.compile(r"\d+")

    def prep_datetime(self, raw):
        m = Preprocessor.re_datetime_regex.findall(raw)
        for d in m:
            dd = ''.join(Preprocessor.re_digits_regex.findall(d))
            if datetime_helper.is_valid_datetime(dd, '%Y%m%d') or datetime_helper.is_valid_datetime(dd, '%m%d%Y'):
                raw = raw.replace(d, "")
        return raw

    money_regex = r"(?:(?<=[\D])\$\d+(?=[\W_]))"
    # isolate_digits_regex = r"(?:[a-z][\s_-][0-9]{,10}[\s_-][a-z])"

    """
    remove digits before unit

    samples:
        I'm 5'6\" 140 lbs.
    """
    units = ['lbs', 'kg', 'hour', 'hr', 'hh']
    unit_regex = r"(?:\d+[\s\W]*(" + r"|".join(units) + "))"

    others_regexes = [
        r"24/7",
        r"#\d+",
        r"\d+\'\d+",
        r"(?<=[\W_])\d{5}[\W_]{1,}\d{5}(?=[\W_])",
        r"- {1,}\d+$",
        r"\d+\%"
    ]
    other_regex = r"(?:" + "|".join(others_regexes) + ")"

    all_regexes = [money_regex, unit_regex, other_regex]
    all_regex = r"(" + r"|".join(all_regexes) + ")"
    re_all_regex = re.compile(all_regex)

    def preprocess(self, raw):
        raw = raw.lower()
        raw = raw.encode('ascii', 'ignore')
        raw = self.prep_datetime(raw)
        # raw = Preprocessor.re_prep.sub(' ', raw)
        raw = Preprocessor.re_all_regex.sub('', raw)
        raw = Preprocessor.re_simple_format.sub(
            'pnwrapper \g<0> pnwrapper', raw)
        # print raw
        return raw


if __name__ == '__main__':
    samples = ['$200tel3365551212',
               '$276 3235551212',
               '07807-254599']

    preprocessor = Preprocessor()
    for sample in samples:
        print preprocessor.preprocess(sample)
