# -*- coding: utf-8 -*-
# @Author: darkshadows123

import re
from sets import Set

def remove_dup(arr):
    return list(set(arr))

def extract_ipaddress(string):
    """Extract ip address from string.
    :param string: the text to extract from
    """
    pattern = r"((([01]?[0-9]?[0-9]|2[0-4][0-9]|25[0-5])[ (\[]?(\.|dot)[ )\]]?){3}([01]?[0-9]?[0-9]|2[0-4][0-9]|25[0-5]))"
    ips = [match[0] for match in re.findall(pattern, string)]

    return remove_dup(ips)


