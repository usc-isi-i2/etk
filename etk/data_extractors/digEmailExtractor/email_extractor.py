# -*- coding: utf-8 -*-
from dig_email_extractor import DIGEmailExtractor


def extract(text, include_context=True):
    dee = DIGEmailExtractor()
    if include_context:
        return dee.extract_email_with_context(text)
    else:
        return dee.extract_email(text)
