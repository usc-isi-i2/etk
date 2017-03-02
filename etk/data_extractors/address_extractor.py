import re

import copy
from digExtractor.extractor import Extractor


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


def cleanAddress(text_string, level):
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


def getNum(text, start, dist):
    end = start + 1
    flag = 0
    while start > 0 and end - start <= dist and text[start] != '\r' and text[start] != '\n':
        if text[start].isdigit() and (start - 1 == 0 or text[start - 1] == " " or text[start - 1] == "\n" or text[start - 1] == ">" or text[start - 1] == ")"):
            flag = 1
            break
        start = start - 1
    return flag, start


def getNumNext(text, end, dist):
    start = end
    flag = 0
    count = 0
    while end < len(text) - 2 and end - start <= dist and text[start] != '\r' and text[start] != '\n':
        if text[end].isdigit():
            count += 1
        if count == 5 and text[end + 1].isdigit() and (end + 1 == len(text) - 2 or text[end + 1] == " " or text[end + 1] == "\n" or text[end + 1] == "<"):
            flag = 1
            break
        end = end + 1
    return flag, end + 1


def getSpace(text, start):
    while start > 0:
        if text[start - 1] == " ":
            break
        start = start - 1
    return start


# class AddressExtractor(Extractor):

#     def __init__(self):
#         super(AddressExtractor, self).__init__()
#         self.renamed_input_fields = 'text'

#     def extract(self, doc):
#         if 'text' in doc:
#             return self.getAddressFromStringType(doc['text'], keywords)
#         return None

#     def get_metadata(self):
#         return copy.copy(self.metadata)

#     def set_metadata(self, metadata):
#         self.metadata = metadata
#         return self

#     def get_renamed_input_fields(self):
#         return self.renamed_input_fields

#     def extractAddress(self, text, p, type1, addresses, offset=0):
#         m = p.search(text, offset)
#         if m is None:
#             return addresses

#         end = m.span()[0] + len(type1) + 1
#         if end != -1:
#             flag = 1
#             flag, bkStart = getNum(text, end - (len(type1) + 1), 50)
#             if flag == 0:
#                 start = getSpace(text, end - (len(type1) + 2))
#             elif flag == 1:
#                 flag, start = getNum(text, bkStart - 1, 10)
#                 if flag == 0:
#                     start = bkStart
#             flag, newEnd = getNumNext(text, end, 25)
#             if flag:
#                 end = newEnd
#             if self.get_include_context():
#                 address = {'value': cleanAddress(text[start:end], 3),
#                            'context': {'start': start,
#                                        'end': end}}
#                 addresses.append(address)

#             else:
#                 addresses.append(cleanAddress(text[start:end], 3))
#             addresses = self.extractAddress(text, p, type1, addresses, end)
#             return addresses
#         return addresses

#     """
#     Input: Text String and keyword python list ex: ["ave","street"] etc.
#     Output: Json object containing input text string with list of
#             associated present addresses

#     Uses keywords list passed as an parameter
#     """

#     def getAddressFromStringType(self, text_string, keywords):
#         addresses = list()
#         text_string_lower = text_string.lower()
#         for k, p in keyword_patterns.iteritems():
#             self.extractAddress(text_string_lower, p, k, addresses)
#         if not self.get_include_context():
#             return list(frozenset(addresses))
#         return addresses





def extractAddress(text, p, type1, addresses, offset=0):
    m = p.search(text, offset)
    if m is None:
        return addresses

    end = m.span()[0] + len(type1) + 1
    if end != -1:
        flag = 1
        flag, bkStart = getNum(text, end - (len(type1) + 1), 50)
        if flag == 0:
            start = getSpace(text, end - (len(type1) + 2))
        elif flag == 1:
            flag, start = getNum(text, bkStart - 1, 10)
            if flag == 0:
                start = bkStart
        flag, newEnd = getNumNext(text, end, 25)
        if flag:
            end = newEnd
        # if self.get_include_context():
        #     address = {'value': cleanAddress(text[start:end], 3),
        #                'context': {'start': start,
        #                            'end': end}}
        #     addresses.append(address)

        # else:
        #     addresses.append(cleanAddress(text[start:end], 3))

        # Removed context flag check
        address = {'value': cleanAddress(text[start:end], 3), 
                    'context': {'start': start, 
                                'end': end}}
        addresses.append(address)

        addresses = extractAddress(text, p, type1, addresses, end)
        return addresses
    return addresses

"""
Input: Text String and keyword python list ex: ["ave","street"] etc.
Output: Json object containing input text string with list of
        associated present addresses

Uses keywords list passed as an parameter
"""

def extract_address(text_string):
    addresses = list()
    text_string_lower = text_string.lower()
    for k, p in keyword_patterns.iteritems():
        extractAddress(text_string_lower, p, k, addresses)
    # if not self.get_include_context():
    #     return list(frozenset(addresses))
    # return addresses

    #Removed context
    return addresses