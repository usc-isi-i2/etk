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
        # remove duplicate '
        extraction = '\''.join([_ for _ in extraction.split('\'') if _])
        extraction = extraction.strip('\'')  # remove following '

        feet, inch = extraction.split('\'', 1)
        ans[HEIGHT_UNIT_FOOT] = int(feet)
        ans[HEIGHT_UNIT_INCH] = int(inch)
    elif 'cm' in extraction:
        value, remaining = extraction.split('cm')
        ans[HEIGHT_UNIT_CENTIMETER] = int(value.strip())
    elif 'ft' in extraction:
        value, remaining = extraction.split('ft')
        ans[HEIGHT_UNIT_FOOT] = float(value.strip())
        if remaining:
            if remaining.isdigit():
                ans[HEIGHT_UNIT_INCH] = int(remaining)
            elif 'in' in remaining:
                value, _ = remaining.split('in')
                ans[HEIGHT_UNIT_INCH] = int(value.strip())
            else:
                print 'WARNING: contain uncatched case:', extraction
    elif '.' in extraction:
        left_part, right_part = extraction.split('.')
        left_part, right_part = int(
            left_part.strip()), int(right_part.strip())
        if left_part >= 4 and left_part <= 6 and right_part >= 1 and right_part <= 11:
            ans[HEIGHT_UNIT_FOOT] = left_part
            ans[HEIGHT_UNIT_INCH] = right_part
        else:
            ans[HEIGHT_UNIT_METER] = left_part
            ans[HEIGHT_UNIT_CENTIMETER] = right_part
    elif extraction.isdigit():
        ans[HEIGHT_UNIT_CENTIMETER] = int(extraction)

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
        if check_value >= 100 and check_value <= 210:   # cm
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

######################################################################
#   Main
######################################################################

def height_extract(text):

    height_extractions = re_us_height.findall(text) + re_ls_height.findall(text)

    height_extractions = remove_dups(
        [normalize_height(_) for _ in height_extractions])

    height = {'raw': height_extractions}

    for target_unit in [HEIGHT_UNIT_CENTIMETER, HEIGHT_UNIT_FOOT]:
        height[target_unit] = [format_output(
            target_unit, _) for _ in transform(height_extractions, target_unit)]

    output = {}

    if len(height['raw']) > 0:
        output['height'] = height

    if 'height' not in output:
        return None

    return output