import re
import types

age = "\s+(?:Age|age|AGE)"
years = "(?:(?:years|yrs|yr|year)(?: old)?|yo(?:$|\s+))"
possessions = "(?:i am|i'm|iam|im)"
r1 = r"age[\s\-~#*=+/_:;]*(\d\d)(?:[^+]|$)"
r2 = r"(\d\d)[^0-9+]*" + years
r3 = r"(?:[^a-zA-Z0-9]+|^)" + possessions + "[\s]+(\d\d)(?:[\s\-~#*=+/_:;,]+|$)"
r4 = age+r"\s+(\d\d)-(\d\d)\s+"
r5 = r"(\d\d)-(\d\d)\s"+years
r6 = r"(\d\d)\s+(?:\w+)\s+(\d\d)\s"+years

regexes = [r1, r2, r3, r4, r5, r6]
regexes = [re.compile(x,re.I) for x in regexes]


def wrap_value_with_context(value, start, end):
    return {'value': value,
            'context': {
                        'start': start,
                        'end': end
                        }
            }


def apply_regex(text, regex):
    extracts = list()
    # To remove duplicate values
    values = set()
    for m in re.finditer(regex, text):
        if m.group(1) not in values:
            extracts.append(wrap_value_with_context(m.group(1),
                                                    m.start(),
                                                    m.end()))
            values.add(m.group(1))
    return extracts


def extract(text):
    extracts = list()
    try:
        if isinstance(regexes, type(re.compile(''))):
            extracts = apply_regex(text, regexes)
        elif isinstance(regexes, types.ListType):
            for r in regexes:
                extracts.extend(apply_regex(text, r))
        return extracts
    except:
        return list()