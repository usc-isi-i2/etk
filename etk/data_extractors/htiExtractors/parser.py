import json
import pkg_resources
import re
import string
import time

from pymongo import MongoClient

import misc
import parser_helpers
import utils


# Initialize global variables
phone_num_sets = misc.phone_num_lists()
countries = utils.countries()
post_id_bp_groups = utils.post_id_backpage_groups()

# read ethnicities into memory for parser to use
with open('dataFiles/ethnicities.json', 'rU') as df:
  eths_dict = json.load(df)


def parse_ethnicity(parts):
  """
    Parse the ethnicity from the Backpage ad. Returns the higher level ethnicities associated with an ethnicity.
    For example, if "russian" is found in the ad, this function will return ["russian", "eastern_european", "white_non_hispanic"].
    This allows for us to look at ethnicities numerically and uniformally.
    Note: The code for this function is pretty old and messy, but still works well enough for our purposes.
      parts -> 
  """

  eastern_european = ['russian', 'ukrainian', 'moldova', 'bulgarian', 'slovakian', 'hungarian', 'romanian',
  'polish', 'latvian', 'lithuanian', 'estonia', 'czech', 'croatian', 'bosnian', 'montenegro', 'macedonian', 'albanian',
  'slovenian', 'serbian', 'kosovo', 'armenian', 'siberian', 'belarusian']
  western_european = ['british', 'german', 'france', 'greek', 'italian', 'belgian', 'netherlands', 'swiss', 'irish',
  'danish', 'sweden', 'finnish', 'norwegian', 'portugese', 'austrian', 'sanmarino', 'turkish', 'liechtenstein', 'australian',
  'newzealand', 'andorra', 'luxembourg', 'israeli', 'jewish']
  caribbean = ['bahamian', 'haitian', 'dominican', 'puertorican', 'jamaican', 'cuban', 'caymanislands', 'trinidad', 'caribbean',
  'guadeloupe', 'martinique', 'barbados', 'saintlucia', 'stlucia', 'curacao', 'aruban', 'saintvincent', 'stvincent', 'creole',
  'grenadines', 'grenada', 'barbuda', 'saintkitts', 'saintmartin', 'anguilla', 'virginislands', 'montserrat', 'saintbarthelemy']
  south_central_american = ['guatemalan', 'belizean', 'honduras', 'nicaraguan', 'elsalvador', 'panamanian', 'costarican',
  'colombian', 'columbian', 'venezuelan', 'ecuadorian', 'peruvian', 'bolivian', 'chilean', 'argentine', 'uruguayan', 'paraguayan',
  'brazilian', 'guyana', 'suriname']
  mexican = ['mexican']
  spanish = ['spanish']
  east_asian = ['thai', 'vietnamese', 'cambodian', 'malaysian', 'filipino', 'singaporean', 'indonesian', 'japanese',
  'chinese', 'taiwanese', 'northkorean', 'southkorean', 'korean']
  korean = ['northkorean', 'southkorean', 'korean']
  south_asian = ['nepalese', 'bangladeshi', 'bhutanese', 'indian']
  hawaiian_pacific_islanders = ['hawaiian', 'guamanian', 'newguinea', 'fiji', 'marianaislands', 'solomonislands', 'micronesia',
  'tuvalu', 'samoan', 'vanuata', 'polynesia', 'cookislands', 'pitcaimislands', 'marshallese']
  middle_eastern = ['iraqi', 'iranian', 'pakistani', 'afghan', 'kazakhstan', 'uzbekistan', 'tajikistan', 'turkmenistan',
  'azerbaijan', 'kyrgyzstan', 'syrian', 'lebanese', 'jordanian', 'saudiarabian', 'unitedarabemirates', 'bahrain', 'kuwait',
  'persian', 'kurdish', 'middleeastern']
  north_african = ['egyptian', 'libyan', 'algerian', 'tunisian', 'moroccan', 'westernsaharan', 'mauritanian', 'senegal', 'djibouti']

  # Broad, high level ethnicity classes
  white_non_hispanic = eastern_european + western_european
  hispanic_latino = caribbean + south_central_american + mexican + spanish
  # Get tribe names!!!
  american_indian = ['nativeamerican', 'canadian', 'alaskan', 'apache', 'aztec', 'cherokee', 'chinook', 'comanche',
  'eskimo', 'incan', 'iroquois', 'kickapoo', 'mayan', 'mohave', 'mojave', 'navaho', 'navajo', 'seminole']
  asian = east_asian + south_asian + hawaiian_pacific_islanders
  midEast_nAfrica = middle_eastern + north_african
  african_american = ['black', 'african american']
  ss_african = ['gambia', 'bissau', 'guinea', 'sierraleone', 'liberian', 'ghana', 'malian', 'burkinafaso', 'beninese', 'nigerian',
  'sudanese', 'eritrea', 'ethiopian', 'cameroon', 'centralafricanrepublic', 'somalian', 'gabon', 'congo', 'ugandan', 'kenyan',
  'tanzanian', 'rwandan', 'burundi', 'angola', 'zambian', 'mozambique', 'malawi', 'zimbabwe', 'namibia', 'botswana',
  'lesotho', 'southafrican', 'swaziland', 'madagascar', 'comoros', 'mauritius', 'saintdenis', 'seychelles', 'saotome']

  # Add various identifying values to Category lists
  white_non_hispanic.append('european')
  white_non_hispanic.append('white')
  hispanic_latino.extend(['hispanic', 'latina'])
  asian.extend(['asian', 'oriental'])
  midEast_nAfrica.extend(['arabian', 'muslim'])

  # "from ____" to handle false positives as names
  from_names = ['malaysia']

  # One massive ethnicities list
  ethnicities = white_non_hispanic + hispanic_latino + american_indian + asian + midEast_nAfrica + african_american + ss_african

  num = 0
  found = []
  # Check each part of the body
  clean_parts = []
  for p in parts:
    part = parser_helpers.clean_part_ethn(p)
    clean_parts.append(part)

    # handle "from _____" ethnicities to avoid false positives in names
    for name in from_names:
      if re.compile(r'from +' + re.escape(name)).search(part):
        found.append(name)

    if any(eth in part for eth in ethnicities):
      # At least one ethnicity was found
      for ethn in ethnicities:
        if ethn in part:
          index=part.index(ethn)
          if (' no ' in part and part.index(' no ')+4==index) or ('no ' in part and part.index('no')==0 and part.index('no ') + 3==index) or ('.no ' in part and part.index('.no ') + 4==index): 
            pass
          else:
          # Found the current ethnicity
            if ethn in ['black', 'african american']:
              ethn = "african_american"
            if ethn == 'white': 
              ethn = 'white_non_hispanic'
            if ethn not in found:
              # Add to Found list, check for subsets
              found.append(ethn)
              if ethn in eastern_european:
                found.append("eastern_european")
              if ethn in western_european:
                found.append("western_european")
              if ethn in caribbean:
                found.append("caribbean")
              if ethn in south_central_american:
                found.append("south_central_american")
              if ethn in east_asian:
                found.append("east_asian")
              if ethn in south_asian:
                found.append("south_asian_indian")
              if ethn in hawaiian_pacific_islanders:
                found.append("hawaiian_pacific_islanders")
              if ethn in middle_eastern:
                found.append("middle_eastern")
              if ethn in north_african:
                found.append("north_african")
              # Check the most general ethnicity categories
              if ethn in white_non_hispanic and "white_non_hispanic" not in found:
                found.append("white_non_hispanic")
                num += 1
              if ethn in hispanic_latino and "hispanic_latino" not in found:
                found.append("hispanic_latino")
                num += 1
              if ethn in american_indian and "american_indian" not in found:
                found.append("american_indian")
                num += 1
              if ethn in asian and "asian" not in found:
                if ethn != "asian":
                  found.append("asian")
                  num += 1
              if ethn in midEast_nAfrica and "midEast_nAfrican" not in found:
                found.append("midEast_nAfrican")
                num += 1
              if ethn in ss_african and "subsaharan_african" not in found:
                found.append("subsaharan_african")
                num += 1
              if ethn == "african_american":
                num += 1

  # Remove ethnicity from all parts
  output_parts = []
  for p in clean_parts:
    part = p
    if any(eth in part for eth in found):
      # Ethnicity(s) found in this part
      for eth in found:
        if eth in part:
          # Remove ethnicity
          part = re.sub(eth, "", part)

    # Add part to output
    if len(part) > 2:
      output_parts.append(part)

  # Check if there was more than one general ethnicity. If so, the ad is multi-racial.
  if num > 1:
    found.append("multiracial")

  found = list(set(found))

  return (found, output_parts)


def parse_indicators(parts, ethnicity):
  """
    Parses terms that may or may not be "indicators" of trafficking. Some terms are used for
    non-trafficking related purposes (e.g. matching or identification problems). Also good to 
    note is that this list has been growing slowly for about 2 years and some indicators have
    been removed/combined. Thus, you may notice that some numeric values are non-existent.

    TODO: Move logic from hard-coded into JSON config file(s).
    
      parts -> The backpage ad's posting_body, separated into substrings
      ethnicity -> The ethnicity list that we parsed for the ad
  """

  ret_val=[]
  for part in parts:
    part=part.lower()
    part = part.replace('virginia', '').replace('fresh pot', '')
    part = re.sub(r'virgin ?island', '', part)
    part = re.sub(r'no teen', '', part)

    if re.compile(r'new ?to ?the ?(usa?|country)').search(part):
      ret_val.append(1)
    if "natasha" in part or "svetlana" in part:
      ret_val.append(2)
    if 'young' in part:
      ret_val.append(3)
    if re.compile(r'just *(hit|turned) *18').search(part):
      ret_val.append(5)
    if re.compile(r'fresh *meat').search(part):
      ret_val.append(6)
    if 'virgin' in part:
      ret_val.append(7)
    if 'foreign' in part:
      ret_val.append(8)
    if re.compile(r'(just|fresh)( *off *)?( *the *)boat').search(part):
      ret_val.append(9)
    if re.compile(r'fresh from over ?sea').search(part):
      ret_val.append(9)
    if re.compile(r'easy *sex').search(part):
      ret_val.append(10)
    if re.compile(r'come *chat *with *me').search(part):
      ret_val.append(11)
    if re.compile(r'\b(massage|nuru)\b').search(part):
      ret_val.append(12)
    if re.compile(r'escort *agency').search(part):
      ret_val.append(13)
    if re.compile(r'((https?)|www)\.\w{5,30}?.com').search(part):
      ret_val.append(14)
    if (re.compile(r'world( )*series').search(part) or re.compile(r'grand( )*slam').search(part) or
       re.compile(r'base( )?ball').search(part) or re.compile(r'double( )?play').search(part) or
       'cws' in part or re.compile(r'home( )?run').search(part) or re.compile(r'batter( )?up').search(part) or
       re.compile(r'triple( )?play').search(part) or re.compile(r'strike( )?out').search(part) or
       'sports' in part):
      ret_val.append(15)
    if (re.compile(r'new ?girls').search(part) or re.compile(r'new ?arrivals').search(part) or
       re.compile(r'just ?in ?from ? \w{3,15}\W').search(part) or re.compile(r'new \w{3,9} staff').search(part)):
      ret_val.append(17)
    if re.compile(r'brand *new').search(part):
      ret_val.append(18)
    if re.compile(r'coll(e|a)ge').search(part) and 15 not in ret_val:
      ret_val.append(19)
    if 'teen' in part:
      ret_val.append(20)
    if re.compile(r'high ?school').search(part):
      ret_val.append(21)
    if re.compile(r'daddy\'?s? ?little ?girl').search(part):
      ret_val.append(22)
    if 'fresh' in part:
      ret_val.append(23)
    phrases = [(r'100%' + re.escape(eth)) for eth in ethnicity]
    if any(re.compile(phrase).search(part) for phrase in phrases):
      ret_val.append(24)
    if re.compile(r'speaks? \d\d? language').search(part):
      ret_val.append(25)
    if re.compile(r'new to the (country|us)').search(part):
      ret_val.append(26)
    if re.compile(r'massage ?parlou?r').search(part):
      ret_val.append(27)
    if re.compile(r'come see us at ').search(part):
      ret_val.append(28)
    if (re.compile(r'420 ?friendly').search(part) or re.compile(r'party ?friendly').search(part) or 
        re.compile(r'420 ?sp').search(part) or ' 420 ' in part):
      ret_val.append(30)
    if 'under 35' in part:
      ret_val.append(31)
    if re.compile(r'\b(avail(able)?|open) *24(/|\\|-)7\b').search(part):
      ret_val.append(33)
    if re.compile(r'no ?(indian)').search(part) or re.compile(r'indians? not ((allow)|(welcome))'):
      ret_val.append(36)
    if re.compile(r'no ?(hispanic|mexican)').search(part) or re.compile(r'(hispanic|mexican)s? not ((allow)|(welcome))'):
      ret_val.append(37)
    if 'incall' in part:
      ret_val.append(38)
    if 'outcall' in part:
      ret_val.append(39)

    parts = part.split('from ')
    parts.pop(0)
    for p in parts:
      p = re.sub(r' +', ' ', p)
      if p.split(' ')[0].lower() in countries:
        ret_val.append(27)
        break
    eastern_euro_countries = ['estonia', 'latvia', 'lithuania', 'armenia', 'russia', 'kazakhstan', 'ukrain', 'belarus',
                              'moldova', 'czech', 'austria', 'croatia', 'hungary', 'poland', 'slovakia', 'slovenia',
                              'albania', 'bosnia', 'bulgaria', 'greece', 'macedonia', 'romania']
    if any(c in part for c in eastern_euro_countries):
      ret_val.append(28)
  ret_val = list(set(ret_val))
  return ret_val


def parse_military_friendly(parts):
  """
    Parse whether or not the ad indicates "military friendly".
      parts -> The backpage ad's posting_body, separated into substrings
  """
  for part in parts:
    if 'military' in part:
      return 1
  return 0


def parse_name(parts, main_names, common_names, debug=0):
  """ 
    Parse all name(s) from a Backpage ad.
      parts -> The backpage ad's posting_body, separated into substrings
      main_names -> "Regular" names (jessica, gabriel, etc.) that can be trusted as names simply by its existence
      common_names -> Names such as "pleasure" or "sexy" that should only be parsed if surrounded by an "intro"
    main_names and common_names can both be either sets or dictionaries. Current version uses dictionaries.
  """

  lowercase_parts = [re.sub(r'(in|out)call', '', p.lower()) for p in parts]

  start = time.time()

  # Intros to common names
  intros = {
    'pre': ['my name is', 'i am', 'call me', 'call', 'text', 'my names', 'my name', 'known as', 'go by', 'Intro',
            'ask for', 'call for', 'ask', 'this is', 'one and only', 'prevphonespothti', 'called'],
    'post': ['is back', 'is here', 'in town', 'prevphonespothti', 'is ready', 'is available']
  }
  spanish_intros = ['hola soy', 'me llamo', 'mi nombre es', 'yo soy', 'pregunta por', 'encantada de conocerlo', 'hola papi']
  intros['pre'].extend(spanish_intros)

  # Regex intros to common names
  rgx_intros = {
    'pre': [r'\b(?:it\'s)\b', r'\b(?:it s)\b', r'\b(?:its)\b', r'\b(?:soy)\b', r'\b(?:es)\b', r'\b(?:hola)\b', r'\b(?:y?o?ur girl)\b', r'\b(?:i\'m)\b',
            r'\b(?:im)\b', r'\b(?:y?o?ur favorite girl)\b', r'\b(?:y?o?ur most favorite girl)\b', r'\bmy ?fr(?:i|e)(?:i|e)nd\b', r'\bm(?:s|z)\.',
            r'\bmi(?:s{1,2}|z{1,2})'],
    'post': [r'\b(?:here)\b', r'\b(?:(?:i|\')s (?:the|my) name)\b']
  }

  # These words shouldn't follow common name matched from an intro
  false_positives = set(['complexion', 'skin', 'hair', 'locks', 'eyes', 'st', 'ave', 'street', 'avenue', 'blvd', 'boulevard',
                         'highway', 'circle', 'hwy', 'road', 'rd'])
  vowels_with_y = set(list('aeiouy'))

  uniques = set([])

  for p in lowercase_parts:
    part = p.lower()
    part = re.sub(r"(^| )i ?'? ?m ", " Intro ", part).strip()
    part = part.replace('<br>', ' ').replace('&amp;', ' and ')
    part = re.sub(r'\.+', ' ', part)
    part = re.sub(r'x+', 'x', part)
    part = re.sub(r'y+', 'y', part)
    # Retain 'part' to be used for separating comma-separated names
    part = re.sub(r',+', ' ', part)
    part = re.sub(r' +', ' ', part)

    builder = []
    for pt in part.split():
      if len(pt) > 2:
        lastc = pt[len(pt)-1]
        # Convert names that have repeated last letters and the last letters aren't "E" and aren't two consonants following a vowel
        if lastc == pt[len(pt)-2] and not (lastc == 'e' or (pt[len(pt)-3] in vowels_with_y and lastc not in vowels_with_y)):
          builder.append(pt[:len(pt)-1])
        else:
          builder.append(pt)
      else:
        builder.append(pt)
    part = ' '.join(builder)

    # Check if the part is entirely just a common word
    ageless_title = re.sub(r' - \d\d', '', part.lower())
    ageless_title = re.sub(r'\W+', '', ageless_title)
    if ageless_title in common_names or ageless_title in main_names:
      uniques.add(ageless_title)
      continue;

    # Find common names that come immediately before or after a "both-side intro"
    for k in intros:
      for intro in intros[k]:
        if intro in part:
          pts = part.split(intro)
          for i in range(1, len(pts)):
            if k == 'post':
              # Check left side of intro
              ptl = re.sub(r'\W', ' ', pts[i-1])
              ptl = re.sub(r' +', ' ', ptl)
              tokenized = ptl.split()
              if tokenized and tokenized[len(tokenized)-1] and tokenized[len(tokenized)-1] in common_names:
                uniques.add(tokenized[len(tokenized)-1])
                break
            else:
              # Check right side of intro
              ptr = re.sub(r'\W', ' ', pts[i])
              ptr = re.sub(r' +', ' ', ptr)
              tokenized = ptr.split()
              if tokenized and tokenized[0] in common_names:
                if not (len(tokenized) > 1 and tokenized[1] in false_positives or (len(tokenized) > 2 and tokenized[2] in false_positives)):
                  # Next 2 words are not false positives
                  uniques.add(tokenized[0])
                  break

    # Check intros that include regexes
    for k in rgx_intros:
      for intro in rgx_intros[k]:
        matches = list(re.findall(intro, part))
        for match in matches:
          pts = part.split(match)
          for i in range(1, len(pts)):
            if k == 'post':
              # Check left side of intro
              ptl = re.sub(r'\W', ' ', pts[i-1])
              ptl = re.sub(r' +', ' ', ptl)
              tokenized = ptl.split()
              if tokenized and tokenized[len(tokenized)-1] and tokenized[len(tokenized)-1] in common_names:
                uniques.add(tokenized[len(tokenized)-1])
                break
            else:
              # Check right side of intro
              ptr = re.sub(r'\W', ' ', pts[i])
              ptr = re.sub(r' +', ' ', ptr)
              tokenized = ptr.split()
              if tokenized and tokenized[0] in common_names:
                if not (len(tokenized) > 1 and tokenized[1] in false_positives or (len(tokenized) > 2 and tokenized[2] in false_positives)):
                  # Next 2 words are not false positives
                  uniques.add(tokenized[0])
                  break

    # Find regular names
    tokens = list(re.split(r'\W+', part))
    for i in range(len(tokens)):
      if not tokens[i]:
        continue
      curr = tokens[i]
      # Check if current token has an 's' at the end (ex: "brittanys beautiful body")
      if curr not in main_names and curr[len(curr)-1] == 's' and curr[:-1] in main_names:
        curr = curr[:-1]
      if curr in main_names:
        # Check if name is a two-part name
        if i > 0 and (''.join([tokens[i-1], curr]) in main_names or ''.join([tokens[i-1], curr]) in common_names):
          # Prev token was a prefix to current
          uniques.add(' '.join([tokens[i-1], curr]))
          uniques.discard(tokens[i-1])
        elif (i < len(tokens)-1 and tokens[i+1] and (''.join([tokens[i], tokens[i+1]]) in main_names or
              ''.join([tokens[i], tokens[i+1]]) in common_names)):
          # Current token has a suffix
          uniques.add(' '.join([tokens[i], tokens[i+1]]))
        elif (i < len(tokens)-1 and tokens[i+1] and tokens[i+1][len(tokens[i+1])-1] == 's' and (''.join([tokens[i], tokens[i+1][:-1]]) in main_names or
              ''.join([tokens[i], tokens[i+1][:-1]]) in common_names)):
          # Current token has a suffix with plural ending ('s')
          uniques.add(' '.join([tokens[i], tokens[i+1][:-1]]))
        else:
          # Only single-word name
          uniques.add(curr)

    # Find common words that are part of "pairing" phrases, paired with names that we found already
    pairings = set(['and', 'plus', 'with'])
    for i in range(len(tokens)):
      if tokens[i] not in uniques and tokens[i] in common_names:
        if i > 1 and tokens[i-2] in uniques and tokens[i-1] in pairings:
          # ex: "jessica and diamond"
          uniques.add(tokens[i])
        elif i < len(tokens)-2 and tokens[i+2] in uniques and tokens[i+1] in pairings:
          # ex: "diamond and jessica"
          uniques.add(tokens[i])

  # Odd cases
  if ('mary' in uniques or 'jane' in uniques or 'mary jane' in uniques) and re.search(r'mary\W+jane', part):
    uniques.discard('jane')
    uniques.discard('mary')
    uniques.discard('mary jane')
  if 'crystal' in uniques and re.search(r'crystal ?(blue|spa|massage|parlor|city|stone)', part):
    uniques.discard('crystal')
      
  # Remove names that are substrings of larger names
  names_final = set([])
  if isinstance(main_names, set):
    # Name datasets are raw sets of names
    for match in uniques:
      if not any(match in name for name in [v for v in uniques if v != match]) and match:
        names_final.add(match.strip())
  else:
    # Name datasets are misspelled names mapped to properly spelled names
    for match in uniques:
      nosp_match = match.replace(' ', '')
      if not any(nosp_match in name for name in [v for v in uniques if v != nosp_match]) and nosp_match:
        # add the parsed name, not the converted one (ex: don't change "mickey" to "mikey")
        names_final.add(nosp_match)

  if debug == 1:
    print 'parse_name time taken: {} seconds'.format(time.time() - start)
  return list(names_final)


def parse_no_blacks(parts):
  """
    Parse whether or not an ad indicates that there are "no black clients" allowed.
    Returns a tuple containing:
      [0]: Binary result of whether or not ad specifies "no blacks allowed"
      [1]: The input strings, minus the sections indicating "no blacks allowed"
  """
  match_patterns = [r'no ?black', r'no ?african', r'no ?aa', r'white ?(guys|men|clients) ?only',
                   r'only ?white ?(guys|men|clients)']

  remove_patterns = [r'no ?black ?or ?african', r'no ?african ?or ?black', r'men', r'guys']

  output_parts = []
  output_val = 0

  # Check each part
  for part in parts:
    o_part = part

    # Check all patterns
    for m in match_patterns:
      found = re.search(m, part)
      if found != None:
        # Found a 'no black allowed' phrase
        output_val = 1

        # Remove all relevant phrases
        for p in remove_patterns:
          o_part = re.sub(p, '', o_part)
        o_part = re.sub(m, '', o_part)

    # Append part to output (if it's not empty)
    if len(o_part) > 2:
      output_parts.append(o_part)
  
  return (output_val, output_parts)


def parse_phone(parts, allow_multiple=False):
  """
    Parse the phone number from the ad's parts
      parts -> The backpage ad's posting_body, separated into substrings
      allow_multiple -> If false, arbitrarily chooses the most commonly occurring phone
  """
  
  # Get text substitutions (ex: 'three' -> '3')
  text_subs = misc.phone_text_subs()
  Small = text_subs['Small']
  Magnitude = text_subs['Magnitude']
  Others = text_subs['Others']

  phone_pattern = r'1?(?:[2-9][0-8][0-9])\s?(?:[2-9][0-9]{2})\s?(?:[0-9]{2})\s?(?:[0-9]{2})'
  phone_pattern_spaces = r'1?\W?[2-9]\W?[0-8]\W?[0-9]\W?[2-9]\W?[0-9]\W?[0-9]\W?[0-9]\W?[0-9]\W?[0-9]\W?[0-9]'

  found_phones = []
  return_parts = []
  # Check each part for phone # and remove from parts if found
  for part in parts:
    body = part
    # remove '420' references to avoid false positives
    body = re.sub(r'420 ?friendly', '', body)
    body = body.replace(' 420 ', '')
    body = body.replace('420 sp', '')

    # Replace all disguising characters in the body
    for key in Small:
      body = re.sub(r'-?'+re.escape(key)+r'-?', str(Small[key]), body)
    for key in Magnitude:
      body = re.sub(r'-?'+re.escape(key)+r'-?', str(Magnitude[key]), body)
    for key in Others:
      body = re.sub(r'-?'+re.escape(key)+r'-?', str(Others[key]), body)
    body = re.sub(r'\W', ' ', body)
    body = re.sub(r' +', ' ', body)

    if len(re.sub(r'\D', '', body)) < 10:
      # Less than 10 numeric digits in part - no phone number here
      return_parts.append(part)
      continue;

    phones = re.findall(phone_pattern, body)

    if len(phones) == 0:
      # No phone number in standard format
      phones = re.findall(phone_pattern_spaces, body)
      if len(phones) > 0:
        # Phone number had spaces between digits
        for found in phones:
          found_phones.append(re.sub(r'\D', '', found))

    else:
      # Found phone in standard format
      for found in phones:
        found_phones.append(re.sub(r'\D', '', found))

    if found_phones:
      # Phone has been found, remove from part)
      for found in found_phones:
        filtered_part = parser_helpers.remove_phone(part, found)
      if re.sub(r'\W', '', filtered_part):
        # get rid of now-empty parts
        return_parts.append(filtered_part)
    else:
      # Phone not found yet, add part to output
      return_parts.append(part)

  if not allow_multiple:
    # Get most commonly occurring phone
    found_phone = ''
    if len(found_phones) > 0:
      found_phone = max(set(found_phones), key=found_phones.count)

    # Return the phone along with the original parts (minus any occurrences of the phone number)
    return (found_phone, return_parts)
  else:
    # return all phones
    return (list(set(found_phones)), return_parts)


def parse_posting_id(text, city):
  """
    Parse the posting ID from the Backpage ad. 
      text -> The ad's HTML (or the a substring containing the "Post ID:" section)
      city -> The Backpage city of the ad
  """
  parts = text.split('Post ID: ')
  if len(parts) == 2:
    post_id = parts[1].split(' ')[0]
    if post_id:
      return post_id + post_id_bp_groups[city]


def parse_time(text):
  """ 
    Parse the time of the ad. 
      text -> The 'posted_date' field of the Backpage ad as input.
  """
  data = text[-15:-7]
  data = data.replace(' ', '')
  return data


def parse_truckers(parts):
  """
    Parse for phrases that indicate a form of "trucker friendly".
      parts -> The backpage ad's posting_body, separated into substrings
    Returns a tuple containing:
      [0]: Binary result of whether or not ad is trucker-friendly
      [1]: The input strings, minus the sections indicating trucker-friendly
  """

  match_terms = ['truck', 'cabs', 'flying j']
  match_patterns = [r'exit /d{1,4}', r'interstate /d{0,3}', r'i-/d/d?/d?', r'iowa ?80']

  output_val = 0
  output_parts = []
  for p in parts:
    part = p
    if any(term in part for term in match_terms):
      # found trucker string
      output_val = 1
      for term in match_terms: 
        # remove matched term
        part = part.replace(term, '')
    else:
      # check for trucker patterns
      for patt in match_patterns:
        found = re.search(patt, part)
        if found != None:
          # found match pattern
          output_val = 1
          part = re.sub(patt, '', part) # remove matched term

    if len(part) > 2:
      output_parts.append(part)

  return (output_val, output_parts)
