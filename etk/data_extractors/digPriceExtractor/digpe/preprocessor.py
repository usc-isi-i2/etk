# -*- coding: utf-8 -*-
# @Author: ZwEin
# @Date:   2016-07-01 13:17:34
# @Last Modified by:   ZwEin
# @Last Modified time: 2016-10-02 15:11:18

import re
import inflection


class ZEPreprocessor():
    def __init__(self):
        pass

    re_space = re.compile(r'\s')
    re_digits = re.compile(r'\d')
    re_single_space = re.compile(
        r'(?:(?<=[ \t])[ \t]+|(?<=[\A\n])[ \t]+|[ \t]+(?=[\Z\n]))')

    # punctuations
    # !"#$%&\'()*+,-./:;<=>?@[\]^_`{|}~
    reg_punctuation = r'(?:[!"#%&\'()*+,-./:;<=>?@[\]^_`{|}~])'

    # mis spell
    numbers = ['zero', 'one', 'two', 'three', 'four',
               'five', 'siz', 'seven', 'eight', 'nine']
    re_twenty_x = re.compile(
        r"(two|twenty[\W_]+(?=(\d|" + r"|".join(numbers) + ")))")
    re_thirty_x = re.compile(
        r"(three|thirty[\W_]+(?=(\d|" + r"|".join(numbers) + ")))")
    re_forty_x = re.compile(
        r"(four|forty[\W_]+(?=(\d|" + r"|".join(numbers) + ")))")
    re_fifty_x = re.compile(
        r"(five|fifty[\W_]+(?=(\d|" + r"|".join(numbers) + ")))")
    re_sixty_x = re.compile(
        r"(six|sixty[\W_]+(?=(\d|" + r"|".join(numbers) + ")))")
    re_seventy_x = re.compile(
        r"(seven|seventy[\W_]+(?=(\d|" + r"|".join(numbers) + ")))")
    re_eighty_x = re.compile(
        r"(eight|eighty[\W_]+(?=(\d|" + r"|".join(numbers) + ")))")
    re_ninety_x = re.compile(
        r"(nine|ninety[\W_]+(?=(\d|" + r"|".join(numbers) + ")))")

    re_ten = re.compile(r"(?<=[ilo0-9\A\n ])ten")
    re_one = re.compile(r'(?:\bper\b|(?<=[0-9yneorxt\A\n ])one)')
    re_zero = re.compile(
        r'(?:\bzero\b|(?<=[0-9])o+\b|\bo+(?=[0-9])|\boh\b|(?:(?<=[o\s])o(?=[o\s])))')

    @staticmethod
    def replace_numeral_words(raw):
        raw = raw.replace("1/2", "half")
        raw = raw.replace("hundred", "00")
        raw = raw.replace("thousand", "000")

        raw = raw.replace("eleven", "11")
        raw = raw.replace("twelve", "12")
        raw = raw.replace("thirteen", "13")
        raw = raw.replace("fourteen", "14")
        raw = raw.replace("fifteen", "15")
        raw = raw.replace("sixteen", "16")
        raw = raw.replace("seventeen", "17")
        raw = raw.replace("eighteen", "18")
        raw = raw.replace("nineteen", "19")

        raw = ZEPreprocessor.re_twenty_x.sub("2", raw)
        raw = ZEPreprocessor.re_thirty_x.sub("3", raw)
        raw = ZEPreprocessor.re_forty_x.sub("4", raw)
        raw = ZEPreprocessor.re_fifty_x.sub("5", raw)
        raw = ZEPreprocessor.re_sixty_x.sub("6", raw)
        raw = ZEPreprocessor.re_seventy_x.sub("7", raw)
        raw = ZEPreprocessor.re_eighty_x.sub("8", raw)
        raw = ZEPreprocessor.re_ninety_x.sub("9", raw)

        raw = ZEPreprocessor.re_ten.sub("10", raw)
        raw = ZEPreprocessor.re_one.sub("1", raw)
        raw = ZEPreprocessor.re_zero.sub("0", raw)

        raw = raw.replace("twenty", "20")
        raw = raw.replace("thirty", "30")
        raw = raw.replace("forty", "40")
        raw = raw.replace("fifty", "50")
        raw = raw.replace("sixty", "60")
        raw = raw.replace("seventy", "70")
        raw = raw.replace("eighty", "80")
        raw = raw.replace("ninety", "90")
        return raw

    irr_units = [
        r'lb',
        r'lbs',
        r'c',
    ]
    # 5'9/160lb/36C
    reg_irr_units = r'(?:(?:\b\d{1,2}\'\d{1,2}\b)|(?:\d+(' + \
        r'|'.join(irr_units) + r')\b))'

    # remove irrelated
    irrelation_regex_list = [
        r'(?:(?<=\d)\.00\b)',               # $160.00 => $160
        r'(?:\d{4,}\.)',                    # 925354849.80 => 80
        r'(?:\d{4,}[a-z]+\d{4,}\.)',        # 92535l4849.80 => 80
        r'(?:\d+[ \t]*%)',                  # 1000%
        reg_irr_units,
        reg_punctuation
    ]
    re_irrelation = re.compile(r'|'.join(irrelation_regex_list))

    # phone numbers
    reg_phone_number = [
        r'(?:\d{7,})',
        r'(?:\d{8}[ ]\d{3})',
        r'(?:\d{7}[ ]\d{4})',
        r'(?:\d{5}[ ]\d{4}[ ]\d{4})',
        r'(?:\d{5}[ ]\d{4}[ ]\d{2}[ ]\d{2})',
        r'(?:\d{4}[ ]\d{4}[ ]\d{2})',
        r'(?:\d{4}[ ]\d{2}[ ]\d{2}[ ]\d{2}[ ]\d{2})',
        r'(?:\d{4}[ ]\d{3}[ ]\d{3})',
        r'(?:\d{3}[ ]\d{7,8})',
        r'(?:\d{3}[ ]\d{4}[ ]\d{4})',
        r'(?:\d{3}[ ]\d{4}[ ]\d{3})',
        r'(?:\d{3}[ ]\d{3}[ ]\d{4})',
        r'(?:\d{3}[ ]\d{3}[ ]\d{3}[ ]\d{1})',
        r'(?:\d{3}[ ]\d{3}[ ]\d{2}[ ]\d{1}[ ]\d{1})',
        r'(?:\d{3}[ ]\d{3}[ ]\d{1}[ ]\d{3})',
        r'(?:\d{3}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{4})',
        r'(?:\d{2}[ ]\d{8,10})',
        r'(?:\d{2}[ ]\d{4}[ ]\d{4})',
        r'(?:\d{2}[ ]\d{1}[ ]\d{8}[ ]\d{1})',
        r'(?:\d{1}[ ]\d{3}[ ]\d{3}[ ]\d{3})',
        r'(?:\d{2}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1})',
        r'(?:\d{1}[ ]\d{2}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1})',
        r'(?:\d{1}[ ]\d{1}[ ]\d{2}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1})',
        r'(?:\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{2}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1})',
        r'(?:\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{2}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1})',
        r'(?:\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{2}[ ]\d{1}[ ]\d{1}[ ]\d{1})',
        r'(?:\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{2}[ ]\d{1}[ ]\d{1})',
        r'(?:\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{2}[ ]\d{1})',
        r'(?:\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{2})',
        r'(?:\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1}[ ]\d{1})'
    ]
    re_phone_number = re.compile(r'|'.join(reg_phone_number))

    def preprocess(self, text):
        text = text.encode('ascii', 'ignore').lower()
        text = ZEPreprocessor.replace_numeral_words(text)
        text = ZEPreprocessor.re_irrelation.sub(' ', text)
        text = ZEPreprocessor.re_single_space.sub('', text)
        text = ZEPreprocessor.re_phone_number.sub('', text)
        text = text.split('\n')
        text = [' '.join([inflection.singularize(token)
                          for token in _.split(' ')]) for _ in text]
        return text
