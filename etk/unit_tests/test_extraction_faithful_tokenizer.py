# coding: utf-8

import unittest
import sys
import os
import json
import codecs
sys.path.append('../../')
sys.path.append('../')
from etk.core import Core


class TestExtractionsUsingRegex(unittest.TestCase):

    def setUp(self):

        self.c = Core()
        self.doc = 'Call meorčpžsíáýd at \n\r  \t   ♥❤⚘sdj,,,?? ?123 fd123-123(123))),345 fdkjf☺☻✌☹♡♥❤⚘❀❃❁✼☀â€™ŰűŲųŴŵŶŷŸŹźŻżŽžſ0180ƀƁƂƃƄƅƆƇƈƉƊƋƌƍƎƏ0190ƐƑƒƓƔƕƖƗƘƙƚƛƜƝƞƟdsfkjhsdf'
        self.correct_tokens = [{'char_end': 8, 'char_start': 4, 'type': 'alphabet', 'value': u'Call'}, {'char_end': 6, 'char_start': 5, 'type': 'break', 'value': u' '}, {'char_end': 29, 'char_start': 17, 'type': 'alphabet', 'value': u'meor\u010dp\u017es\xed\xe1\xfdd'}, {'char_end': 19, 'char_start': 18, 'type': 'break', 'value': u' '}, {'char_end': 22, 'char_start': 20, 'type': 'alphabet', 'value': u'at'}, {'char_end': 38, 'char_start': 29, 'type': 'break', 'value': u' \n\r  \t   '}, {'char_end': 35, 'char_start': 32, 'type': 'emoji', 'value': u'\u2665\u2764\u2698'}, {'char_end': 38, 'char_start': 35, 'type': 'alphabet', 'value': u'sdj'}, {'char_end': 36, 'char_start': 35, 'type': 'punctuation', 'value': u','}, {'char_end': 37, 'char_start': 36, 'type': 'punctuation', 'value': u','}, {'char_end': 38, 'char_start': 37, 'type': 'punctuation', 'value': u','}, {'char_end': 39, 'char_start': 38, 'type': 'punctuation', 'value': u'?'}, {'char_end': 40, 'char_start': 39, 'type': 'punctuation', 'value': u'?'}, {'char_end': 42, 'char_start': 41, 'type': 'break', 'value': u' '}, {'char_end': 42, 'char_start': 41, 'type': 'punctuation', 'value': u'?'}, {'char_end': 48, 'char_start': 45, 'type': 'digit', 'value': u'123'}, {'char_end': 47, 'char_start': 46, 'type': 'break', 'value': u' '}, {'char_end': 50, 'char_start': 48, 'type': 'alphabet', 'value': u'fd'}, {'char_end': 54, 'char_start': 51, 'type': 'digit', 'value': u'123'}, {'char_end': 52, 'char_start': 51, 'type': 'punctuation', 'value': u'-'}, {'char_end': 58, 'char_start': 55, 'type': 'digit', 'value': u'123'}, {'char_end': 56,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                'char_start': 55, 'type': 'punctuation', 'value': u'('}, {'char_end': 62, 'char_start': 59, 'type': 'digit', 'value': u'123'}, {'char_end': 60, 'char_start': 59, 'type': 'punctuation', 'value': u')'}, {'char_end': 61, 'char_start': 60, 'type': 'punctuation', 'value': u')'}, {'char_end': 62, 'char_start': 61, 'type': 'punctuation', 'value': u')'}, {'char_end': 63, 'char_start': 62, 'type': 'punctuation', 'value': u','}, {'char_end': 69, 'char_start': 66, 'type': 'digit', 'value': u'345'}, {'char_end': 68, 'char_start': 67, 'type': 'break', 'value': u' '}, {'char_end': 77, 'char_start': 72, 'type': 'alphabet', 'value': u'fdkjf'}, {'char_end': 98, 'char_start': 85, 'type': 'emoji', 'value': u'\u263a\u263b\u270c\u2639\u2661\u2665\u2764\u2698\u2740\u2743\u2741\u273c\u2600'}, {'char_end': 89, 'char_start': 87, 'type': 'alphabet', 'value': u'\xe2\u20ac'}, {'char_end': 89, 'char_start': 88, 'type': 'emoji', 'value': u'\u2122'}, {'char_end': 120, 'char_start': 104, 'type': 'alphabet', 'value': u'\u0170\u0171\u0172\u0173\u0174\u0175\u0176\u0177\u0178\u0179\u017a\u017b\u017c\u017d\u017e\u017f'}, {'char_end': 112, 'char_start': 108, 'type': 'digit', 'value': u'0180'}, {'char_end': 140, 'char_start': 124, 'type': 'alphabet', 'value': u'\u0180\u0181\u0182\u0183\u0184\u0185\u0186\u0187\u0188\u0189\u018a\u018b\u018c\u018d\u018e\u018f'}, {'char_end': 132, 'char_start': 128, 'type': 'digit', 'value': u'0190'}, {'char_end': 177, 'char_start': 152, 'type': 'alphabet', 'value': u'\u0190\u0191\u0192\u0193\u0194\u0195\u0196\u0197\u0198\u0199\u019a\u019b\u019c\u019d\u019e\u019fdsfkjhsdf'}]
        self.correct_simple_tokens = [u'Call', u' ', u'meor\u010dp\u017es\xed\xe1\xfdd', u' ', u'at', u' \n\r  \t   ', u'\u2665\u2764\u2698', u'sdj', u',', u',', u',', u'?', u'?', u' ', u'?', u'123', u' ', u'fd', u'123', u'-', u'123',
                                      u'(', u'123', u')', u')', u')', u',', u'345', u' ', u'fdkjf', u'\u263a\u263b\u270c\u2639\u2661\u2665\u2764\u2698\u2740\u2743\u2741\u273c\u2600', u'\xe2\u20ac', u'\u2122', u'\u0170\u0171\u0172\u0173\u0174\u0175\u0176\u0177\u0178\u0179\u017a\u017b\u017c\u017d\u017e\u017f', u'0180', u'\u0180\u0181\u0182\u0183\u0184\u0185\u0186\u0187\u0188\u0189\u018a\u018b\u018c\u018d\u018e\u018f', u'0190', u'\u0190\u0191\u0192\u0193\u0194\u0195\u0196\u0197\u0198\u0199\u019a\u019b\u019c\u019d\u019e\u019fdsfkjhsdf']

    def test_extractions_structured_tokens(self):
        extracted_tokens = self.c.extract_tokens_faithful(self.doc)

        extracted_simple_tokens = self.c.extract_tokens_from_faithful(
            extracted_tokens)

        extracted_doc = ''.join(extracted_simple_tokens)
        # Check for structured tokens
        self.assertEquals(extracted_tokens, self.correct_tokens)

        # Check for simple tokens
        self.assertEquals(extracted_simple_tokens, self.correct_simple_tokens)

        # Check for string
        self.assertEquals(extracted_doc, self.doc.decode('utf-8'))

if __name__ == '__main__':
    unittest.main()
