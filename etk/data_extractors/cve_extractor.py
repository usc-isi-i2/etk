# -*- coding: utf-8 -*-
# @Author: darkshadows123

import re
from sets import Set

def remove_dup(arr):
    return list(set(arr))

def extract_cve(string):
    """Extract cve from string.
    :param string: the text to extract from
    """
    pattern = r"CVE-(\d{4})-(\d{4})"
    cves = [cve.group(0) for cve in re.finditer(pattern, string)]
    cves = remove_dup(cves)

    return cves
