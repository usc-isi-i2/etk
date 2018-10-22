import json
import pkg_resources
import re
import string
import sys

import misc
from unicode_decoder import decode_unicode


phone_num_sets = misc.phone_num_lists()

# read ethnicities into memory for parser to use
with open('dataFiles/ethnicity_replacements.json', 'rU') as df:
  eth_subs = json.load(df)


# Helper method for find_price_by_time
def check_price(bod_parts, factor):
  # Iterate through parts and find price if it exists
  split_parts = bod_parts
  options = []
  l_index = 0
  r_index = 1
  while r_index < len(split_parts):
    # Found a price indicator
    index = -1
    match = re.sub(r"""\D""", ' ', split_parts[l_index][-6:]) 
    if len(match.replace(" ", "")) == 0:
      # Check right side if the left side doesn't contain integer.
      match = re.sub(r"""\D""", ' ', split_parts[r_index][:6])
      index = 0

    match = re.sub(r""" +""", ' ', match)
    pts = match.split(" ")
    parts = []

    for pt in pts:
      if pt != "":
        parts.append(pt)
    if len(parts) > 1:
      # There were more than two possible integers. Choose the correct one.
      if index == -1:
        index = len(parts) - 1
      match = parts[index]
    else:
      # Either 0 or 1 integer values found.
      match = match.replace(" ", "")

    if len(match) > 0:
      if int(match) >= 20 and (int(match) * factor) < 9000:
        options.append(int(match) * factor)

    if index == 0:
      # Only increment the indeces if we have checked both left and right sides.
      l_index += 1
      r_index += 1
    else:
      # Get rid of left side
      split_parts[l_index] = ""

  if len(options) > 1:
    return options[get_correct_price(bod_parts)]
  elif len(options) == 1:
    return options[0]
  # No price found
  return -1


def clean_part_ethn(body):
  """
    Prepare a string to be parsed for ethnicities.

    Returns a "translated" string (e.g. all instances of "china" converted to "chinese")
  """

  # patterns that can create false positive situations
  patterns_to_remove = [r'black ?or ?african', r'african ?or ?black', r'no ?black', r'no ?african', r'no ?aa', r'white ?men', 
  r'white ?gentlemen', r'no ?spanish', r'speak ?spanish', r'black ?(guys|men|hair|client)', r'dark ?hair', 
  r'(dark ?)?brown ?hair', r'white ?tie']

  # indian states to convert the term 'indian'
  indian_states = ['awadhi', 'badhi', 'bhutia', 'garhwali', 'halbi', 'kamboj', 'bhattarai', 'bhotiya', 'pardeshi',
  'bengali', 'madra', 'tamil', 'rajasthani', 'adivasi']

  for p in patterns_to_remove:
    body = re.sub(p, '', body)
  for i in indian_states:
    body = body.replace(i, 'indian')

  # regex substitutions
  body = re.sub(r'hong ?kong', 'chinese', body)
  body = re.sub(r'snow ?bunn(y|ies)', 'white', body)
  body = re.sub(r'a\ss\si\sa\sn', 'asian', body)
  body = re.sub(r'l\sa\st\si\sn\sa', 'latina', body)

  # convert many ethnicity variations into standardized ones (e.g. china -> chinese)
  for sub in eth_subs:
    body = body.replace(sub, eth_subs[sub])

  body = re.sub(r' +', ' ', body)

  return body


# Helper that filters bodies of text - used by get_clean_parts
def clean_str(text):
  output = re.sub(r'<[^<]+?>', '', text) # remove HTML
  output = re.sub(r' *\\r\\n *', '', output) # remove \r\n inline tags
  output = re.sub(r'(\r?\n)+', 'QXZ', output) # replace \n with QXZ - the regex to split the string with
  output = re.sub(r'_+', ' ', output) # remove _ symbols
  output = re.sub(r'\(+', '(', output)
  output = re.sub(r'\)+', ')', output)
  output = output.replace('&amp;', ' and ')
  output = re.sub(r'\s+', ' ', output, re.U) # remove excess white space

  # Filter out various troublesome words
  remove_terms = ["&lt;", "&gt;", "*", "polished", "car stay", "holiday", "low as", "minimum", "unrushed"]

  # regex patters to remove
  remove_patterns = [r"(boy|best) ?friend", r"starting ?at\b", r"price ?of\b", r"donations?", r"\b(24|48) ?h(ou)?r",
  r"hour ?wait", r"overnight ?special", r"\d{2,4} *%", r"\d{1,2}\s?(a|p)m\b", r"\d{2,3}lbs?",
  r'\bfun\b', r'(mon|tues|wednes|thurs|fri|sat|sun|turkey)day', r'\bfriendly\b']
  # words that are enclosed in spaces
  remove_patterns.append(r' (per|s|for|car) ')
  # remove decimals in prices (ex: change $100.99 to $100)
  remove_patterns.append(r'(?<=\b\d\d)\.\d{2}\b')

  for term in remove_terms:
    output = output.replace(term, ' ')

  for rgx in remove_patterns:
    output = re.sub(rgx, ' ', output)

  # reduce duplicate spaces
  output = re.sub(' +', ' ', output)

  return output


# Helper that removes any parts that may have blank or only-symbolic contents
def filter_bad_parts(parts):
  output = []
  for part in parts:
    filtered = re.sub(r'\W', '', part)
    if filtered:
      pt = part.strip()
      pt = re.sub(r'(?<=\w) +,', ',', pt)
      output.append(pt)

  return output


def get_clean_loc_parts(text, is_location=False):
  if not is_location:
    body = json.dumps(text.lower())
    # get specific part of HTML text that contains neighborhood info
    location_section = get_location_section(body)
  else:
    location_section = text

  if not location_section:
    return []
  body = decode_unicode(location_section, replace_boo=False)[1]

  # remove unicode, format
  literal_body = body.encode('unicode_escape').decode('unicode_escape')
  body = clean_str(literal_body)
  remove_terms = ["\\", '"', "[", "]", "{", "}"]
  body = re.sub(r'(only)? ?(out|in)calls? ?(only)?', ' ', body)
  body = re.sub(r'(^| )(out|in)($| )', ' ', body)
  body = re.sub(r'^ *$', '', body)
  body = re.sub(r'( ,)+', ',', body)
  body = re.sub(r'location:,?', '', body)
  body = re.sub(r'city of,?', '', body)
  body = re.sub(r',+', ',', body)
  for term in remove_terms:
    body = body.replace(term, "")
  body = re.sub(r' +', ' ', body)
  body = body.strip()
  # convert to clean list of neighborhoods
  return body.split(",")


# Removes all Unicode and HTML from both title and body of ad. Returns list separated by new lines that were typed in.
def get_clean_parts(body, title, split_complete=False, keep_upper=False):
  t = title
  if not keep_upper:
    t = t.lower()
  b = body
  if not keep_upper:
    b = b.lower()

  clean_title = clean_str(t)
  clean_body = clean_str(b)

  bod_parts = clean_body.split("QXZ")
  bod_parts.append(clean_title)

  bod_parts = filter_bad_parts(bod_parts)

  if split_complete:
    finished_parts = []
    for part in bod_parts:
      finished_parts.extend(part.split('. '))
    bod_parts = finished_parts
  
  return bod_parts


def get_location_section(text):
  location_pattern = re.compile(r"""location:.*?</div>""", re.VERBOSE)
  arr = location_pattern.findall(text)
  if len(arr) == 0:
    return []
  loc_section = ''.join(['"', arr[0], '"'])
  if not loc_section:
    # handle unfound section that would otherwise still be a list
    return ''
  return loc_section


# digit == "two" or "8"
# digit_index = index in phone number that we are checking
def recursive_find(reduced_part, digit, digit_index, phone, original_part):
  if len(reduced_part) == 0:
    # Was not found
    return original_part

  if digit in reduced_part:
    new_reduced = reduced_part[reduced_part.index(digit) + len(digit):]

    if digit_index == (len(phone) - 1):
      # Found last digit - return everything after that
      return new_reduced

    # Recursively find next digit
    next_digit_terms = phone_num_sets[phone[digit_index + 1]]
    for d in next_digit_terms:
      ret = recursive_find(new_reduced, d, digit_index + 1, phone, original_part)
      if ret != original_part:
        # Return if it was found previously
        return ret
  
  # Digit not in this reduced_part
  return original_part


# Remove the parsed phone number from the original ad
def remove_phone(part, phone):
  filtered = part
  text_subs = misc.phone_text_subs()

  # Replace all disguising characters in the body in order to check if there are even 10 digits
  Small = text_subs['Small']
  Magnitude = text_subs['Magnitude']
  Others = text_subs['Others']
  for key in Small:
    filtered = re.sub(r" ?" + key + r" ?", str(Small[key]), filtered)
  for key in Magnitude:
    filtered = re.sub(key, str(Magnitude[key]), filtered)
  for key in Others:
    filtered = re.sub(key, str(Others[key]), filtered)
  filtered = re.sub(r"""\W""", ' ', filtered)
  filtered = re.sub(r""" +""", ' ', filtered)

  if len(re.sub("\D", "", filtered)) < 10:
    # Less than 10 numeric digits in part - no phone number here
    return part

  # Check standard regex first (all numeric)
  regex = ""
  for char in phone:
    regex = regex + char + "\D*"
  regex = regex[:-3]
  if len(re.sub(regex, "", part)) < len(part):
    # Found phone pattern with all numerics
    parts = re.sub(regex, "QYZ", part).split("QYZ")
    beg = parts[0]
    end = parts[1]
    if "call" in beg[-8:]:
      ind = beg.index('call')
      # Capture the index of the LAST appearance of the word "call"
      while ind < len(beg)-1 and 'call' in beg[ind+1:]:
        ind = beg.index('call', ind+1)
      beg = beg[:ind]
    if "call" in end[:8]:
      end = end[end.index("call") + 4:]
    return beg + ' prevphonespothti ' + end

  # Look for phone using all forms of digits (ex: "three" for '3')
  for n in phone_num_sets[phone[0]]:
    if n in part:
      start_index = part.index(n)
      checked = recursive_find(part[start_index:], n, 0, phone, part)
      if checked != part:
        # Found the phone!
        beg = part[:start_index]
        end = checked
        if "call" in beg[-8:]:
          beg = beg[beg.index("call"):]
        if "call" in end[:8]:
          end = end[end.index("call") + 4:]
        return beg + ' prevphonespothti ' + end
  # Phone wasn't found
  return part

