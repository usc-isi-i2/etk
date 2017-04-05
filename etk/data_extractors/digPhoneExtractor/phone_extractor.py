# -*- coding: utf-8 -*-

from pnmatcher import PhoneNumberMatcher


def extract(tokens, source_type='text', include_context=True, _output_format='obfuscation'):
    extractor = PhoneNumberMatcher(_output_format=_output_format)
    extracts = []
    extracts += extractor.match(tokens, source_type=source_type, include_context=include_context)
    return extracts


