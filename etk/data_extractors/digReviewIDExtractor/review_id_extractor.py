# -*- coding: utf-8 -*-
# @Author: ZwEin
# @Date:   2016-07-22 17:52:30
# @Last Modified by:   ZwEin
# @Last Modified time: 2016-10-02 15:17:21

import re

######################################################################
#   Constant
######################################################################

RE_DICT_NAME_IDENTIFIER = 'identifier'
RE_DICT_NAME_SITE = 'site'

RE_DICT_SITE_NAME_NR = 'nr'
RE_DICT_SITE_NAME_TER = 'ter'
RE_DICT_SITE_NAME_411 = '411'
RE_DICT_SITE_NAME_OTHERS = 'others'

RE_DICT_SITE = [
    RE_DICT_SITE_NAME_NR,
    RE_DICT_SITE_NAME_TER,
    RE_DICT_SITE_NAME_411,
    RE_DICT_SITE_NAME_OTHERS
]

punctuations = r'\!\"\!\"\$\%\'\(\)\*\+\,\-\.\/\:\;\<\=\>\?\@\[\\\]\^\_\`\{\|\}\~'
keywords = [
    'review',
    'revews',
    'reviews',
    'reviewed',
    'reviewedid'
]


######################################################################
#   Regular Expression
######################################################################

re_tokenize = re.compile(r'[\s'+punctuations+r']')
re_seperator = re.compile(r'[\n]')

reg_simpleones = [
    r'(?<=\n\n)\d{6}$'
]
re_simpleones = re.compile(r'(?:'+r'|'.join(reg_simpleones)+r')', re.IGNORECASE)

reg_keywords = r'|'.join(keywords)
reg_word_gap = r'(?:[a-z]+ ){,3}?'
reg_fc_word_prev = r'(?:'+reg_keywords+r') '+reg_word_gap
reg_fc_word_post = reg_word_gap+r'?(?:'+reg_keywords+r')'
reg_fc_simple = r'#'
reg_fc_ter = r'(?:\bt\s*?[e3]\b|\b[e3]\s*?p\b|\bt\s*?[e3]?\s*?r(?:id|r)?\b|\bt\b|\be\b|\br\b)'
reg_fc_id = r'i[\s'+punctuations+r']{,5}?d\s*?'
reg_back_check = r'[a-z]{,7}(?![\s'+punctuations+r']*?\d)'
reg_target = r'.{,2}?(?:(?<!\d)\d{6}(?!\d)[\s\&]*)+'


reg_rid = [
    r'(?:'+reg_fc_ter+r'?\s*?'+reg_fc_simple+reg_target+reg_back_check+r')',
    r'(?:\b'+reg_fc_ter+reg_word_gap+reg_target+reg_back_check+r')',
    r'(?:\b'+reg_fc_ter+r'?\s*?'+reg_fc_id+reg_word_gap+reg_target+reg_back_check+r')',
    r'(?:\b'+reg_fc_ter+r'.{,5}?'+reg_fc_id+reg_target+reg_back_check+r')',
    r'(?:'+reg_fc_ter+r'?\s*?'+reg_fc_word_prev+reg_target+reg_back_check+r')'
]
re_rid = re.compile(r'(?:'+r'|'.join(reg_rid)+r')', re.IGNORECASE)
re_digits = re.compile(r'(?:\d{6})')

# normalize
re_nr = re.compile(r'(?:\bn\s*?r)', re.IGNORECASE)
re_ter = re.compile(reg_fc_ter, re.IGNORECASE)
re_411 = re.compile(r'(?:\bp\d{5}\b)', re.IGNORECASE)
re_others = re.compile(r'.*', re.IGNORECASE)

re_sites = {
    RE_DICT_SITE_NAME_NR: re_nr,
    RE_DICT_SITE_NAME_TER: re_ter,
    RE_DICT_SITE_NAME_411: re_411,
    RE_DICT_SITE_NAME_OTHERS: re_others
}

def create_output(review_id, site):
    out = dict()
    out["metadata"] = dict()
    out["metadata"]["site"] = site
    out["value"] = review_id
    return out


def extract(text):
    ans = []
    ans += [{RE_DICT_NAME_IDENTIFIER:_, RE_DICT_NAME_SITE: RE_DICT_SITE_NAME_OTHERS} for _ in re_simpleones.findall(text)]
    ans += [{RE_DICT_NAME_IDENTIFIER:_, RE_DICT_NAME_SITE: RE_DICT_SITE_NAME_411} for _ in re_411.findall(text)]

    texts = re_seperator.split(text)
    potentials = []
    for text in texts:
        text = ' '.join([_.strip() for _ in re_tokenize.split(text) if _.strip() != ''])
        potentials += re_rid.findall(text)

    for p in set(potentials):
        for ext in re_digits.findall(p):
            extraction = None
            for site in RE_DICT_SITE:
                if re_sites[site].findall(p):
                    extraction = create_output(ext, site)
                    break
            if extraction:
                ans.append(extraction)
            else:
                ans.append(create_output(ext, RE_DICT_SITE_NAME_OTHERS))

    return ans

#
# text = 'Hey guys! I\'m Heidi!!! I am a bubbly, busty, blonde massage therapist and only provide the most sensual therapeutic experience! I love what I do and so will YOU!!! I am always learning new techniqes and helping other feel relaxed. Just send Me an email and lets meet!!!  I am reviewed! #263289 \nheidishandsheal@gmail.com'
#
# print extract(text)