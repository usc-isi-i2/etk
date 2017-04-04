# -*- coding: utf-8 -*-
# @Author: ZwEin
# @Date:   2016-06-15 21:05:39
# @Last Modified by:   ZwEin
# @Last Modified time: 2016-10-01 09:44:28

from datetime import datetime

def is_valid_datetime(raw, date_format):
    try:
        datetime.strptime(raw, date_format)
        return True
    except ValueError:
        return False