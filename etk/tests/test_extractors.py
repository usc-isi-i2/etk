import sys
import os
sys.path.insert(1, os.path.join(sys.path[0], '../..'))
import pytest
import json
import etk
#from digExtractor.extractor_processor import ExtractorProcessor
#from digAgeRegexExtractor.age_regex_helper import get_age_regex_extractor

tk = etk.init()

def get_age_test_cases():
	f = open('test_cases/age.json','r')
	data = f.read().split('\n')
	test_cases = []

	for t in data:
		t = json.loads(t)
		test_cases.append((t['content'],t['correct']))
	return test_cases

@pytest.mark.parametrize('doc,extraction',get_age_test_cases())
def test_age_extractor(doc,extraction):
	extracted_ages = tk.extract_age(doc)
	extracted_ages = [age['value'] for age in extracted_ages]
	assert set(extracted_ages) == set(extraction)