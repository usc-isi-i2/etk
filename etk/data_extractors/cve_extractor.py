# -*- coding: utf-8 -*-
# @Author: darkshadows123

import re
from sets import Set

list_of_values = []


def wrap_value_with_context(value, start, end):
    return {
            'value': value,
            'context': {
                        'start': start,
                        'end': end
                        }
            }

def extract_cve(string):
    """Extract cve from string.
    :param string: the text to extract from
    """
    pattern = r"CVE-(\d{4})-(\d{4})"
    cves = list()
    h_map = list()
    for cve in re.finditer(pattern, string):
        if cve.group(0) not in h_map:
            cves.append(wrap_value_with_context(cve.group(0),cve.start(),cve.end()))
        h_map.append(cve.group(0))
    return cves
