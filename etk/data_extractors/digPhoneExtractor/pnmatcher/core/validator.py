# -*- coding: utf-8 -*-
# @Author: ZwEin
# @Date:   2016-06-14 16:17:20
# @Last Modified by:   ZwEin
# @Last Modified time: 2016-12-08 11:39:08

"""
ensure phone numbers are valid

"""
import re
import phonenumbers
from phonenumbers.phonenumberutil import NumberParseException
from pnmatcher.core.common import datetime_helper
from difflib import SequenceMatcher
# from pnmatcher.res import area_code


class Validator():

    re_zero = re.compile(r'0{4,}')

    def validate_phone_number_with_coutry_code(self, raw, country_code='US'):
        try:
            z = phonenumbers.parse(raw, country_code)
        except NumberParseException, e:
            pass

            """
            if e.error_type == NumberParseException.INVALID_COUNTRY_CODE:
                # Invalid country code specified
                return []
            elif e.error_type == NumberParseException.NOT_A_NUMBER:
                # The string passed in had fewer than 3 digits in it.
                # The number failed to match the regular expression
                return []
            elif e.error_type == NumberParseException.TOO_SHORT_AFTER_IDD:
                # The string started with an international dialing prefix
                # but after this was removed, it had fewer digits than any
                # valid phone number (including country code) could have.
                return []
            elif e.error_type == NumberParseException.TOO_SHORT_NSN:
                # After any country code has been stripped, the string
                # had fewer digits than any valid phone number could have.
                return []

            elif e.error_type == NumberParseException.TOO_LONG:
                # String had more digits than any valid phone number could have
                return []
            """

            # print e.error_type, e._msg
        else:
            if phonenumbers.is_possible_number(z) and phonenumbers.is_valid_number(z):
                return [raw]
            else:
                return []

    def validate_phone_number(self, raw):
        # match all countries if using area_code.get_all_country_iso_two_letter_code()
        # may include too short phone numbers if use 'DE'
        country_code_list = ['US', 'CN', 'IN', 'UA',
                             'JP', 'RU', 'IT', 'DE', 'CA', 'TR']
        for country_code in country_code_list:
            rtn = self.validate_phone_number_with_coutry_code(
                raw, country_code=country_code)
            if rtn:
                return rtn

    def is_datetime(self, raw):

        size = len(raw)

        date_format = ''
        if size == 14:
            return datetime_helper.is_valid_datetime(raw, '%Y%m%d%H%M%S')
        elif size == 8:
            return datetime_helper.is_valid_datetime(raw, '%Y%m%d')
        elif size == 6:
            return datetime_helper.is_valid_datetime(raw, '%Y%m%d') or datetime_helper.is_valid_datetime(raw, '%H%M%S')
        else:
            return False

    re_num_digits = [
        None,
        re.compile(r"\d{1}"),
        re.compile(r"\d{2}"),
        re.compile(r"\d{3}"),
        re.compile(r"\d{4}"),
        re.compile(r"\d{5}"),
        re.compile(r"\d{6}")
    ]

    def is_all_dup_digits(self, raw):
        for i in range(1, 6):
            rtn = Validator.re_num_digits[i].findall(raw)
            if len(raw) % i != 0:
                continue
            if all(rtn[0] == rest for rest in rtn):
                return True
        return False

    re_start_zero = re.compile(r'^0+')

    def suggest_most_overlap(self, extracted_phone_list):
        def similar(a, b):
            return SequenceMatcher(None, a, b).ratio()
        potential_invalid, potential_valid = [], []
        for pn in extracted_phone_list:
            if len(pn) >= 9 and len(pn) <= 13:
                potential_valid.append(pn)
            else:
                potential_invalid.append(pn)
        ans = list(potential_valid)
        for pi in potential_invalid:
            if any(similar(pi, pv) > .3 for pv in potential_valid):
                ans.append(pi)
        return ans

    def validate(self, raw):

        ans = []
        for nums in raw.split('\t'):
            nums = nums.strip()
            nums = Validator.re_start_zero.sub('', nums)

            if len(nums) > 16:
                continue
                
            if len(Validator.re_zero.findall(nums)):
                continue

            if self.is_all_dup_digits(nums):
                continue

            if self.is_datetime(nums):
                continue

            ans += [nums]

            # valid = self.validate_phone_number(nums)
            # if valid:
            #     ans.extend(valid)

        ans = list(set(ans))
        ans = self.suggest_most_overlap(ans)

        return ' '.join(ans)
