import re
import types


years = "(?:(?:years|yrs|yr|year)(?: old)?|yo(?:$|\s+))"
posessions = "(?:i am|i'm|iam|im)"
r1 = r"age[\s\-~#*=+/_:;]*(\d\d)(?:[^+]|$)"
r2 = r"(\d\d)[^0-9+]*" + years
r3 = r"(?:[^a-zA-Z0-9]+|^)" + posessions + "[\s]+(\d\d)(?:[\s\-~#*=+/_:;,]+|$)"
regexes = [r1, r2, r3]
regexes = [re.compile(x) for x in regexes]

def wrap_value_with_context(value, field, start, end):
	return {'value': value,
			'context': {'field': field,
						'start': start,
						'end': end
						}
			}


def apply_regex(text, regex):
		extracts = list()
		for m in re.finditer(regex, text):
				extracts.append(wrap_value_with_context(m.group(1),
															 'text',
															 m.start(),
															 m.end()))
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