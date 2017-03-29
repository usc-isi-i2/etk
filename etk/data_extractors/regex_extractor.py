import re
import types


def wrap_value_with_context(value, field, start, end):
    return {'value': value,
            'context': {'field': field,
                        'start': start,
                        'end': end
                        }
            }


def apply_regex(text, regex, include_context, flags):
    extracts = list()
    for m in re.finditer(regex, text, flags=flags):
        if include_context:
            extracts.append(wrap_value_with_context(m.group(1), 'text', m.start(), m.end()))
        else:
            extracts.append(m.group(1))
    return extracts


def extract(text, regex, include_context=True,flags=0):
    extracts = list()
    try:
        if isinstance(regex, type(re.compile(''))) or isinstance(regex, basestring):
            print "here1"
            extracts = apply_regex(text, regex, include_context, flags)
            print include_context
            print flags
            print regex
            print extracts
        elif isinstance(regex, types.ListType):
            print "here2"
            for r in regex:
                extracts.extend(apply_regex(text, r, include_context, flags))
        if include_context:
            return extracts
        else:
            return list(frozenset(extracts))
    except Exception as e:
        print e
        return list()
