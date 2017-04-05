# -*- coding: utf-8 -*-
# @Author: ZwEin
# @Date:   2016-06-30 11:29:35
# @Last Modified by:   ZwEin
# @Last Modified time: 2016-10-02 15:12:34

from preprocessor import ZEPreprocessor
from extractor import ZEExtractor
from normalizer import ZENormalizer
from unit import * 
import re


PE_DICT_NAME_PRICE = 'price'
PE_DICT_NAME_PPH = 'price_per_hour'

PE_JSON_NAME_PRICE = 'price'
PE_JSON_NAME_PRICE_UNIT = 'price_unit'
PE_JSON_NAME_TIME_UNIT = 'time_unit'

class DIGPriceExtractor():

    def __init__(self):
        self.preprocessor = ZEPreprocessor()
        self.extractor = ZEExtractor()
        self.normalizer = ZENormalizer()

    re_digits = re.compile(r'\d+')
    re_alphabet = re.compile(r'[a-z ]+')

    def extract(self, text):
        cleaned_text_list = self.preprocessor.preprocess(text)
        extracted_text_list = self.extractor.extract_from_list(cleaned_text_list)
        normalized_text_list = self.normalizer.normalize_from_list(extracted_text_list)
        ans = {}
        ans.setdefault(PE_DICT_NAME_PRICE, [])
        ans.setdefault(PE_DICT_NAME_PPH, [])
        for normalized in normalized_text_list:
            if not normalized[PE_JSON_NAME_TIME_UNIT]:
                ans[PE_DICT_NAME_PPH].append(normalized[PE_JSON_NAME_PRICE])
            else:
                tunit = DIGPriceExtractor.re_alphabet.findall(normalized[PE_JSON_NAME_TIME_UNIT])
                if tunit and tunit[0].strip() in UNIT_TIME_HOUR:
                    if tunit[0].strip() in UNIT_TIME_HOUR:
                        digits = DIGPriceExtractor.re_digits.findall(normalized[PE_JSON_NAME_TIME_UNIT])
                        if not digits or int(digits[0]) == 1:
                            # ans.append(normalized)
                            ans[PE_DICT_NAME_PPH].append(normalized[PE_JSON_NAME_PRICE])

            ans[PE_DICT_NAME_PRICE].append(normalized)
        return ans


    def extract_from_list(self, text_list):
        return [self.extract(text) for text in text_list]
