'''
#from digRegexExtractor.regex_extractor import RegexExtractor

years = "(?:(?:years|yrs|yr|year)(?: old)?|yo(?:$|\s+))"
posessions = "(?:i am|i'm|iam|im)"
r1 = r"age[\s\-~#*=+/_:;]*(\d\d)(?:[^+]|$)"
r2 = r"(\d\d)[^0-9+]*" + years
r3 = r"(?:[^a-zA-Z0-9]+|^)" + posessions + "[\s]+(\d\d)(?:[\s\-~#*=+/_:;,]+|$)"
regexes = [r1, r2, r3]

def get_age_regex_extractor():
    #return RegexExtractor().set_regex(regexes)\
    #                       .set_metadata({'extractor': 'age regex'})

def extract_age():
	"""
	To do
	Onload functions from - 
		RegexExtractor and 
		ExtractorProcessor
	"""
'''	