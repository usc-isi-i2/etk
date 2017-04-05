import re
import types

age = "\s+(?:Age|age|AGE)"
years = "(?:(?:years|yrs|yr|year)(?: old)?|yo(?:$|\s+))"
posessions = "(?:i am|i'm|iam|im|me|about)"
r1 = age+r"[\s\-~#*=+/_:;]*(\d\d)(?:[^+]|$)"
r2 = r"(\d\d)\s*(?:\+)?" + years
r3 = r"(?:[^a-zA-Z0-9]+|^)" + posessions + "[\s]+(\d\d)(?:[\s\-~#*=+/_:;,]+|$)"
r4 = age+r"\s+(\d\d)-(\d\d)\s+"
r5 = r"(\d\d)-(\d\d)\s"+years
r6 = r"(\d\d)\s+(?:\w+)\s+(\d\d)\s"+years

regexes = [r1, r2, r3, r4, r5,r6]
regexes = [re.compile(x,re.I) for x in regexes]

def wrap_value_with_context(value, field, start, end):
	return {'value': value,
			'context': {'field': field,
						'start': start,
						'end': end
						}
			}


def apply_regex(text, regex):
		extracts = list()
		#To remove duplicate values
		values = set()
		for m in re.finditer(regex, text):
			for age in m.groups():
				if(age not in values):
					extracts.append(wrap_value_with_context(age,
															 'text',
															 text.index(age),
															 text.index(age)+len(age)))
					values.add(age)
		return extracts



def extract(doc, regex):
	try:
		if isinstance(regex, type(re.compile(''))):
			extracts = apply_regex(doc, regex)
		elif isinstance(regex, types.ListType):
			extracts = list()
			for r in regex:
				extracts.extend(apply_regex(doc, r))
		return (extracts)
	except:
	    return list()


def age_extract(doc):

	updated_doc = extract(doc, regexes)

	return updated_doc