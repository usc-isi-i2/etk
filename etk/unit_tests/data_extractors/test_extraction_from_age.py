# -*- coding: utf-8 -*-
import unittest
import sys

sys.path.append('../../')
sys.path.append('../')
from etk.core import Core
import json


class TestExtractionFromAge(unittest.TestCase):
    def setUp(self):
        f = open('ground_truth/age.jl','r')

        data = f.read().split('\n')
        self.doc = []

        for t in data:
            self.doc.append(json.loads(t))

    def test_extraction_from_age(self):

        c = Core()

        for t in self.doc:
            extracted_ages = c._extract_age(t['content'])
            extracted_ages = [age['value'] for age in extracted_ages]
            self.assertEquals(set(extracted_ages), set(t['correct']))

if __name__ == '__main__':
    unittest.main()