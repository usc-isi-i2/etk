import re

######################################################################
#   Constant
######################################################################

# Constants for weight
WEIGHT_UNIT_POUND = 'pound'
WEIGHT_UNIT_KILOGRAM = 'kilogram'

WEIGHT_UNIT_POUND_ABBRS = ['lb', 'lbs']
WEIGHT_UNIT_KILOGRAM_ABBRS = ['kg']

WEIGHT_UNITS_DICT = {
    WEIGHT_UNIT_POUND: WEIGHT_UNIT_POUND_ABBRS,
    WEIGHT_UNIT_KILOGRAM: WEIGHT_UNIT_KILOGRAM_ABBRS
}

# Transform
WEIGHT_TRANSFORM_DICT = {
    (WEIGHT_UNIT_POUND, WEIGHT_UNIT_POUND): 1,
    (WEIGHT_UNIT_POUND, WEIGHT_UNIT_KILOGRAM): 0.45359237,
    (WEIGHT_UNIT_KILOGRAM, WEIGHT_UNIT_POUND): 2.2046,
    (WEIGHT_UNIT_KILOGRAM, WEIGHT_UNIT_KILOGRAM): 1,
}

######################################################################
#   Regular Expression
######################################################################

reg_us_weight_unit_lb = r'\b\d{2,3}[ ]*(?:lb|lbs)\b'
reg_us_weight_unit_kg = r'\b\d{2}[ ]*kg\b'

re_us_weight = re.compile(r'(?:' + r'|'.join([
    reg_us_weight_unit_lb,
    reg_us_weight_unit_kg
]) + r')', re.IGNORECASE)

reg_ls_weight = r'(?<=weight)[: \n]*' + r'(?:' + r'|'.join([
    reg_us_weight_unit_lb,
    reg_us_weight_unit_kg,
    r'(?:\d{1,3})'
]) + r')'

re_ls_weight = re.compile(reg_ls_weight, re.IGNORECASE)


######################################################################
#   Main Class
######################################################################




def clean_extraction(extraction):
    extraction = extraction.replace(':', '')
    extraction = extraction.lower().strip()
    return extraction


def remove_dups(extractions):
    return [dict(_) for _ in set([tuple(dict_item.items()) for dict_item in extractions if dict_item])]


######################################################################
#   Normalize
######################################################################

def normalize_weight(extraction):
    extraction = clean_extraction(extraction)
    ans = {}
    if 'lb' in extraction:
        value, remaining = extraction.split('lb')
        ans['value'] = int(value.strip())
        ans['unit'] = WEIGHT_UNIT_POUND
    elif 'kg' in extraction:
        value, remaining = extraction.split('kg')
        ans['value'] = int(value.strip())
        ans['unit'] = WEIGHT_UNIT_KILOGRAM
    elif extraction.isdigit():
        if len(extraction) == 2:
            ans['value'] = int(extraction)
            ans['unit'] = WEIGHT_UNIT_KILOGRAM
        elif len(extraction) == 3:
            ans['value'] = int(extraction)
            ans['unit'] = WEIGHT_UNIT_POUND
        else:
            return None

    return ans


######################################################################
#   Unit Transform
######################################################################

def transform(extractions, target_unit):
    ans = []
    for extraction in extractions:
        imd_value = 0.
        for (unit, value) in extraction.iteritems():
            imd_value += WEIGHT_TRANSFORM_DICT[(unit, target_unit)] * value
        if sanity_check(target_unit, imd_value):
            ans.append(imd_value)
    return ans


######################################################################
#   Sanity Check
######################################################################

def sanity_check(unit, value):
    if (unit, WEIGHT_UNIT_KILOGRAM) in WEIGHT_TRANSFORM_DICT:
        check_value = WEIGHT_TRANSFORM_DICT[
                          (unit, WEIGHT_UNIT_KILOGRAM)] * value
        if check_value >= 30 and check_value <= 200:  # kg
            return True
    return False


######################################################################
#   Output Format
######################################################################

def format_output(value):
    return int(value)


def wrap_value_with_context(value, start, end):
    return {'value': value,
            'context': {'start': start,
                        'end': end
                        }
            }


def apply_regex(text, regex):
    extracts = list()
    for m in re.finditer(regex, text):
        extracts.append(wrap_value_with_context(m.group(),
                                                m.start(),
                                                m.end()))
    return extracts


def remove_dup(extractions):
    value_set = set()
    weight_extractions = []
    for i in range(len(extractions)):
        extractions[i]['value'] = extractions[i]['value'].strip()
        if extractions[i]['value'] not in value_set:
            value_set.add(extractions[i]['value'])
            result = normalize_weight(extractions[i]['value'])
            if result:
                extractions[i]['value'] = str(result['value'])
                extractions[i]['metadata'] = dict()
                extractions[i]['metadata']['unit'] = result['unit']
                weight_extractions.append(extractions[i])
        else:
            continue
    return weight_extractions


######################################################################
#   Main
######################################################################


def extract(text):
    us_h = apply_regex(text, re_us_weight)
    ls_h = apply_regex(text, re_ls_weight)

    weight_extractions = us_h + ls_h
    weight_extractions = remove_dup(weight_extractions)

    return weight_extractions

