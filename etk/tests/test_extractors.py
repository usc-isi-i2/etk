import sys
import os
sys.path.insert(1, os.path.join(sys.path[0], '../..'))
import pytest
import json
import etk
#from digExtractor.extractor_processor import ExtractorProcessor
#from digAgeRegexExtractor.age_regex_helper import get_age_regex_extractor

tk = etk.init()
print tk


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


'''
def get_age_test_cases():
	f = open('test_cases/age.json','r')
	data = f.read().split('\n')
	test_cases = []

	for t in data:
		t = json.loads(t)
		test_cases.append(t)
	return test_cases

test_cases = get_age_test_cases()
for t in test_cases:
	doc = t
	extractor = get_age_regex_extractor()
	extractor_processor = ExtractorProcessor().set_input_fields('content').set_output_field('extracted').set_extractor(extractor)
	updated_doc = extractor_processor.extract(doc)
	original = []
	if('extracted' in updated_doc):
		original = [i['value'] for i in updated_doc['extracted'][0]['result']]
	
	extraction = tk.extract_age(t['content'])	
	e = [i['value'] for i in extraction]
	if(set(e)!=set(t['correct'])):
		print e,original,t['correct']

'''