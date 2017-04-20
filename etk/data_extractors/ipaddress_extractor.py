# -*- coding: utf-8 -*-
# @Author: darkshadows123

import re
from sets import Set

def wrap_value_with_context(value, start, end):
    return {
            'value': value,
            'context': {
                        'start': start,
                        'end': end
                        }
            }

def extract_ipaddress(string):
    """Extract ip address from string.
    :param string: the text to extract from
    """
    pattern = r"((([01]?[0-9]?[0-9]|2[0-4][0-9]|25[0-5])[ (\[]?(\.|dot)[ )\]]?){3}([01]?[0-9]?[0-9]|2[0-4][0-9]|25[0-5]))"
    ips = list()
    h_map = list()
    for match in re.finditer(pattern, string):
        if match.group(0) not in h_map:
            ips.append(wrap_value_with_context(match.group(0),match.start(),match.end()))
        h_map.append(match.group(0))

    return ips    	



