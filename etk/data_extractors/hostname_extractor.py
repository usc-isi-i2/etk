# -*- coding: utf-8 -*-
# @Author: darkshadows123

import re
from sets import Set

################################################
# Main
################################################

"""Extractor of md5 hash from text.

Users of this class should call extract_hostname(), see documentation.
"""
def remove_dup(arr):
    return list(set(arr))

def extract_hostname(string):
    """Extract all hostnames from string.
    :param string: the text to extract from
    """
    pattern = r'(?:[a-zA-Z0-9](?:[a-zA-Z0-9\-]{,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,6}'
    hostnames = re.findall(pattern, string)

    return remove_dup(hostnames)

