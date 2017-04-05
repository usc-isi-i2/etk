# -*- coding: utf-8 -*-
# @Author: ZwEin
# @Date:   2016-06-13 23:15:52
# @Last Modified by:   ZwEin
# @Last Modified time: 2016-10-01 09:44:07

"""
clean misspelling number words and replace numeral words

"""

import re


class Cleaner():

    def prep_misspelled_numeral_words(self, raw):
        misspelling_dict = {
            "th0usand": "thousand",
            "th1rteen": "thirteen",
            "f0urteen": "fourteen",
            "e1ghteen": "eighteen",
            "n1neteen": "nineteen",
            "f1fteen": "fifteen",
            "s1xteen": "sixteen",
            "th1rty": "thirty",
            "e1ghty": "eighty",
            "n1nety": "ninety",
            "fourty": "forty",
            "f0urty": "forty",
            "e1ght": "eight",
            "f0rty": "forty",
            "f1fty": "fifty",
            "s1xty": "sixty",
            "zer0": "zero",
            "for": "four",
            "f0ur": "four",
            "f1ve": "five",
            "n1ne": "nine",
            "0ne": "one",
            "too": "two",
            "tw0": "two",
            "to": "two",
            "s1x": "six"
        }

        for key in misspelling_dict.keys():
            raw = raw.replace(key, misspelling_dict[key])
        return raw

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

    # re_ten = re.compile(r"(?<=[ilo0-9])ten(?=[ ilo0-9\.\t\(\),\:\-\+\!])")
    # re_ten = re.compile(r"(?<=[ilo0-9])ten\b")
    re_ten = re.compile(r"(?<=[ilo0-9])ten(?=[ \b0-9])")
    re_one = re.compile(
        r'(?:(?<=([0-9yneorxt]| ))one|(?:(?<=[ils])[i]((?=[ils])|$)))')
    re_zero = re.compile(
        r'(?:zero|oh|(?:(?<=[0-9])(o+?))|(?:o(?=[0-9]))|(?:(?<=[o\s])o(?=[o\s])))')

    def prep_replace_numeral_words(self, raw):
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

        raw = Cleaner.re_twenty_x.sub("2", raw)
        raw = Cleaner.re_thirty_x.sub("3", raw)
        raw = Cleaner.re_forty_x.sub("4", raw)
        raw = Cleaner.re_fifty_x.sub("5", raw)
        raw = Cleaner.re_sixty_x.sub("6", raw)
        raw = Cleaner.re_seventy_x.sub("7", raw)
        raw = Cleaner.re_eighty_x.sub("8", raw)
        raw = Cleaner.re_ninety_x.sub("9", raw)

        raw = Cleaner.re_ten.sub("10", raw)
        raw = Cleaner.re_one.sub("1", raw)
        raw = Cleaner.re_zero.sub("0", raw)

        raw = raw.replace("twenty", "20")
        raw = raw.replace("thirty", "30")
        raw = raw.replace("forty", "40")
        raw = raw.replace("fifty", "50")
        raw = raw.replace("sixty", "60")
        raw = raw.replace("seventy", "70")
        raw = raw.replace("eighty", "80")
        raw = raw.replace("ninety", "90")
        return raw

    def clean(self, raw):
        raw = self.prep_misspelled_numeral_words(raw)
        raw = self.prep_replace_numeral_words(raw)
        # print raw
        return raw
