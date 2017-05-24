# -*- coding: utf-8 -*-
# @Author: darkshadows123

import re
from sets import Set

################################################
# Main
################################################

def wrap_value_with_context(value, start, end):
    return {
            'value': value,
            'context': {
                        'start': start,
                        'end': end
                        }
            }

"""Extractor of hostname from text.

Users of this class should call extract_hostname(), see documentation.
"""

def extract_hostname(string):
    """Extract all hostnames from string.
    :param string: the text to extract from
    """
    pattern = r'(?:[a-zA-Z0-9](?:[a-zA-Z0-9\-]{,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,6}'
    hostnames = []
    h_map = list()
    for match in re.finditer(pattern, string):
        if match.group(0) not in h_map:
            hostnames.append(wrap_value_with_context(match.group(0),match.start(),match.end()))
        h_map.append(match.group(0))

    return hostnames    	

