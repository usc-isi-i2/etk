import re

######################################################################
#   Constant
######################################################################

# Constants for height
HEIGHT_UNIT_METER = 'meter'
HEIGHT_UNIT_CENTIMETER = 'centimeter'
HEIGHT_UNIT_FOOT = 'foot'
HEIGHT_UNIT_INCH = 'inch'

HEIGHT_UNIT_CENTIMETER_ABBRS = ['cm']
HEIGHT_UNIT_FOOT_ABBRS = ['ft']
HEIGHT_UNIT_INCH_ABBRS = ['in']

HEIGHT_UNITS_DICT = {
    HEIGHT_UNIT_CENTIMETER: HEIGHT_UNIT_CENTIMETER_ABBRS,
    HEIGHT_UNIT_FOOT: HEIGHT_UNIT_FOOT_ABBRS,
    HEIGHT_UNIT_INCH: HEIGHT_UNIT_INCH_ABBRS
}

# Transform
HEIGHT_TRANSFORM_DICT = {
    (HEIGHT_UNIT_METER, HEIGHT_UNIT_METER): 1,
    (HEIGHT_UNIT_METER, HEIGHT_UNIT_CENTIMETER): 100,
    (HEIGHT_UNIT_METER, HEIGHT_UNIT_FOOT): (1 / 0.3048),
    (HEIGHT_UNIT_METER, HEIGHT_UNIT_INCH): (1 / 0.0254),
    (HEIGHT_UNIT_CENTIMETER, HEIGHT_UNIT_METER): 0.01,
    (HEIGHT_UNIT_CENTIMETER, HEIGHT_UNIT_CENTIMETER): 1,
    (HEIGHT_UNIT_CENTIMETER, HEIGHT_UNIT_FOOT): (1 / 30.48),
    (HEIGHT_UNIT_CENTIMETER, HEIGHT_UNIT_INCH): (1 / 2.54),
    (HEIGHT_UNIT_FOOT, HEIGHT_UNIT_METER): 0.3048,
    (HEIGHT_UNIT_FOOT, HEIGHT_UNIT_CENTIMETER): 30.48,
    (HEIGHT_UNIT_FOOT, HEIGHT_UNIT_FOOT): 1,
    (HEIGHT_UNIT_FOOT, HEIGHT_UNIT_INCH): 12,
    (HEIGHT_UNIT_INCH, HEIGHT_UNIT_METER): 0.0254,
    (HEIGHT_UNIT_INCH, HEIGHT_UNIT_CENTIMETER): 2.54,
    (HEIGHT_UNIT_INCH, HEIGHT_UNIT_FOOT): (1. / 12),
    (HEIGHT_UNIT_INCH, HEIGHT_UNIT_INCH): 1
}

######################################################################
#   Regular Expression
######################################################################

# Reg for target with unit, 'us': unit solution
reg_us_height_symbol = r'(?:\b\d\'{1,2}[ ]*\d{1,2}\b)'

reg_us_height_unit_cm = r'(?:\b\d{3}[ ]*cm\b)'
reg_us_height_unit_ft = r'(?:(?:\b\d{1}\.\d{1}[ ]*ft)|(?:\b\d{1}[ ]*ft))[ ]*(?:(?:\d{1}in\b)?|\b)'

re_us_height = re.compile(r'(?:' + r'|'.join([
    reg_us_height_symbol,
    reg_us_height_unit_cm,
    reg_us_height_unit_ft
]) + r')', re.IGNORECASE)

# Reg for target after label height or weight, 'ls': label solution
reg_ls_height = r'(?<=height)[: \n]*' + r'(?:' + r'|'.join([
    reg_us_height_unit_cm,
    reg_us_height_unit_ft,
    reg_us_height_symbol,
    r'(?:\d{1}\.\d{1,2})',
    r'(?:\d{1,3})'
]) + r')'

re_ls_height = re.compile(reg_ls_height, re.IGNORECASE)


######################################################################
#   Main Class
######################################################################




######################################################################
#   Clean
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

def normalize_height(extraction):
    extraction = clean_extraction(extraction)
    ans = {}
    if '\'' in extraction:
        extraction = '\''.join([_ for _ in extraction.split('\'') if _])
        extraction = extraction.strip('\'')  # remove following '

        feet, inch = extraction.split('\'', 1)
        ans['value'] = str(feet) + "'" + str(inch) + '"'
        ans['unit'] = HEIGHT_UNIT_FOOT + '/' + HEIGHT_UNIT_INCH

    elif 'cm' in extraction:
        value, remaining = extraction.split('cm')
        ans['value'] = int(value.strip())
        ans['unit'] = HEIGHT_UNIT_CENTIMETER

    elif 'ft' in extraction:
        value, remaining = extraction.split('ft')
        ans['value'] = str(float(value.strip()))
        ans['unit'] = HEIGHT_UNIT_FOOT
        if remaining:
            if remaining.isdigit():
                ans['value'] += "'" + str(int(remaining)) + '"'
                ans['unit'] += '/' + HEIGHT_UNIT_INCH
            elif 'in' in remaining:
                value, _ = remaining.split('in')
                ans['value'] += '\'' + value.strip() + '"'
                ans['unit'] += '/' + HEIGHT_UNIT_INCH

            else:
                print 'WARNING: contain uncatched case:', extraction
    elif '.' in extraction:
        left_part, right_part = extraction.split('.')
        left_part, right_part = int(
            left_part.strip()), int(right_part.strip())
        if left_part >= 4 and left_part <= 6 and right_part >= 1 and right_part <= 11:
            ans['unit'] = HEIGHT_UNIT_FOOT + '/' + HEIGHT_UNIT_INCH
            ans['value'] = str(left_part) + '.' + str(right_part)
        else:
            ans['unit'] = HEIGHT_UNIT_METER + '/' + HEIGHT_UNIT_CENTIMETER
            ans['value'] = str(left_part) + '.' + str(right_part)
    elif extraction.isdigit():
        ans['value'] = int(extraction)
        ans['unit'] = HEIGHT_UNIT_CENTIMETER

    return ans


######################################################################
#   Unit Transform
######################################################################

def transform(extractions, target_unit):
    ans = []
    for extraction in extractions:
        imd_value = 0.
        for (unit, value) in extraction.iteritems():
            imd_value += HEIGHT_TRANSFORM_DICT[(unit, target_unit)] * value
        if sanity_check(target_unit, imd_value):
            ans.append(imd_value)
    return ans


######################################################################
#   Sanity Check
######################################################################

def sanity_check(unit, value):
    if (unit, HEIGHT_UNIT_CENTIMETER) in HEIGHT_TRANSFORM_DICT:
        check_value = HEIGHT_TRANSFORM_DICT[
                          (unit, HEIGHT_UNIT_CENTIMETER)] * value
        if check_value >= 100 and check_value <= 210:  # cm
            return True
    return False


######################################################################
#   Output Format
######################################################################

def format_output(target_unit, value):
    if target_unit == HEIGHT_UNIT_FOOT:
        ft_value = str(value)
        if '.' in ft_value:
            left_part, right_part = ft_value.split('.')
            return '{0}\'{1}"'.format(left_part.strip(), int(12 * float('.' + right_part.strip())))
        else:
            return ft_value + '\''
    return int(value)


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
        extracts.append(wrap_value_with_context(m.group(),
                                                'text',
                                                m.start(),
                                                m.end()))
    return extracts


def remove_dup(extractions):
    value_set = set()
    height_extractions = []
    for i in range(len(extractions)):
        extractions[i]['value'] = extractions[i]['value'].strip()
        if (extractions[i]['value'] not in value_set):
            value_set.add(extractions[i]['value'])
            extractions[i]['value'] = normalize_height(extractions[i]['value'])
            height_extractions.append(extractions[i])
        else:
            continue
    return height_extractions


######################################################################
#   Main
######################################################################

def height_extract(text):
    us_h = apply_regex(text, re_us_height)
    ls_h = apply_regex(text, re_ls_height)
    height_extractions = us_h + ls_h

    height_extractions = remove_dup(height_extractions)
    return height_extractions
