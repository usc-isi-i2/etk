import re
"""
Keywords: all the street types we want to match on.
"""

keywords = ["avenue", "blvd", "boulevard", "pkwy", "parkway", "way",
            "st", "street", "rd", "road", "drive", "lane", "alley", "ave"]

keyword_patterns = dict()

for each_keyword in keywords:
        p = re.compile(r'\b%s\b' % each_keyword.lower(), re.I)
        keyword_patterns[each_keyword] = p

phonePattern = re.compile(r'(\d{3})\D*(\d{3})\D*(\d{4})')


def clean_address(text_string, level):
    if level > 0:
        # Slicing from 'location' word in found adddress
        pos = text_string.find('location')
        if pos > -1:
            text_string = text_string[pos + len('location'):]

    if level > 1:
        # Slicing from phone number reference word in found adddress to end
        m = phonePattern.search(text_string)
        if m is not None:
            pos = m.span()[1]
            text_string = text_string[pos:]

    if level > 2:
        # Cleaning if maps URL present
        if text_string.find('maps.google.com') > -1 or text_string.find('=') > -1:
            pos = text_string.rfind('=')
            if pos > -1:
                text_string = text_string[pos + 1:].replace('+', ' ')
    return text_string.strip()


def get_num(text, start, dist):
    end = start + 1
    flag = 0
    while start > 0 and end - start <= dist and text[start] != '\r' and text[start] != '\n':
        if text[start].isdigit() and (start - 1 == 0 or text[start - 1] == " " or text[start - 1] == "\n" or text[start - 1] == ">" or text[start - 1] == ")"):
            flag = 1
            break
        start = start - 1
    return flag, start


def get_num_next(text, end, dist):
    start = end
    flag = 0
    count = 0
    while end < len(text) - 2 and end - start <= dist and text[start] != '\r' and text[start] != '\n':
        if text[end].isdigit():
            count += 1
        if count == 5 and text[end + 1].isdigit() and (end + 1 == len(text) - 2 or text[end + 1] == " " or text[end + 1] == "\n" or text[end + 1] == "<"):
            flag = 1
            break
        end += 1
    return flag, end + 1


def getSpace(text, start):
    while start > 0:
        if text[start - 1] == " ":
            break
        start -= 1
    return start


def extract_address(text, p, type1, addresses, offset=0):
    m = p.search(text, offset)
    if m is None:
        return addresses

    end = m.span()[0] + len(type1) + 1
    if end != -1:
        flag = 1
        flag, bkStart = get_num(text, end - (len(type1) + 1), 50)
        if flag == 0:
            start = getSpace(text, end - (len(type1) + 2))
        elif flag == 1:
            flag, start = get_num(text, bkStart - 1, 10)
            if flag == 0:
                start = bkStart
        flag, newEnd = get_num_next(text, end, 25)
        if flag:
            end = newEnd

        # Removed context flag check
        address = {'value': clean_address(text[start:end], 3),
                    'context': {'start': start, 
                                'end': end}}
        addresses.append(address)

        addresses = extract_address(text, p, type1, addresses, end)
        return addresses
    return addresses

"""
Input: Text String and keyword python list ex: ["ave","street"] etc.
Output: Json object containing input text string with list of
        associated present addresses

Uses keywords list passed as an parameter
"""


def extract(text_string):
    addresses = list()
    text_string_lower = text_string.lower()
    for k, p in keyword_patterns.iteritems():
        extract_address(text_string_lower, p, k, addresses)

    return addresses
