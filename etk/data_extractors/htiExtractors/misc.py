
def phone_num_lists():
  """
  Gets a dictionary of 0-9 integer values (as Strings) mapped to their potential Backpage ad manifestations, such as "zer0" or "seven".

  Returns:
    dictionary of 0-9 integer values mapped to a list of strings containing the key's possible manifestations

  """

  all_nums = {}
  all_nums['2'] = ['2', 'two']
  all_nums['3'] = ['3', 'three']
  all_nums['4'] = ['4', 'four', 'fuor']
  all_nums['5'] = ['5', 'five', 'fith']
  all_nums['6'] = ['6', 'six']
  all_nums['7'] = ['7', 'seven', 'sven']
  all_nums['8'] = ['8', 'eight']
  all_nums['9'] = ['9', 'nine']
  all_nums['0'] = ['0', 'zero', 'zer0', 'oh', 'o']
  all_nums['1'] = ['1', 'one', '!' 'l', 'i']

  return all_nums


def phone_text_subs():
  """
  Gets a dictionary of dictionaries that each contain alphabetic number manifestations mapped to their actual
  Number value.

  Returns:
    dictionary of dictionaries containing Strings mapped to Numbers

  """

  Small = {
    'zero': 0,
    'zer0': 0,
    'one': 1,
    'two': 2,
    'three': 3,
    'four': 4,
    'fuor': 4,
    'five': 5,
    'fith': 5,
    'six': 6,
    'seven': 7,
    'sven': 7,
    'eight': 8,
    'nine': 9,
    'ten': 10,
    'eleven': 11,
    'twelve': 12,
    'thirteen': 13,
    'fourteen': 14,
    'fifteen': 15,
    'sixteen': 16,
    'seventeen': 17,
    'eighteen': 18,
    'nineteen': 19,
    'twenty': 20,
    'thirty': 30,
    'forty': 40,
    'fifty': 50,
    'sixty': 60,
    'seventy': 70,
    'eighty': 80,
    'ninety': 90,
    'oh': 0
  }

  Magnitude = {
    'thousand': 000,
    'million': 000000,
  }

  Others = {
    '!': 1,
    'o': 0,
    'l': 1,
    'i': 1
  }

  output = {}
  output['Small'] = Small
  output['Magnitude'] = Magnitude
  output['Others'] = Others

  return output
