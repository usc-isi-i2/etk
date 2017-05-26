"""
This class is used to speed up general, day-to-day programming needs. It contains a variety of 
very commonly used functions - anything from retrieving a custom list of dates to 
retrieving Dictionaries of Backpage cities' coordinates. 

"""
from copy import deepcopy
import csv
from datetime import datetime, timedelta
import pkg_resources
import re

from pymongo import MongoClient


def all_cities():
  """
  Get a list of all Backpage city names.

  Returns:
    list of city names as Strings
  """
  cities = []
  fname = pkg_resources.resource_filename(__name__, 'resources/CityPops.csv')
  with open(fname, 'rU') as csvfile:
    reader = csv.reader(csvfile, delimiter = ',')
    for row in reader:
      cities.append(row[0])
  cities.sort()
  return cities


def all_days(boo):
  """
  Return a list of all dates from 11/12/2015 to the present.

  Args:
    boo: if true, list contains Numbers (20151230); if false, list contains Strings ("2015-12-30")
  Returns:
    list of either Numbers or Strings
  """
  earliest = datetime.strptime(('2015-11-12').replace('-', ' '), '%Y %m %d')
  latest = datetime.strptime(datetime.today().date().isoformat().replace('-', ' '), '%Y %m %d')
  num_days = (latest - earliest).days + 1
  all_days = [latest - timedelta(days=x) for x in range(num_days)]
  all_days.reverse()

  output = []

  if boo:
    # Return as Integer, yyyymmdd
    for d in all_days:
      output.append(int(str(d).replace('-', '')[:8]))
  else:
    # Return as String, yyyy-mm-dd
    for d in all_days:
      output.append(str(d)[:10])
  return output


def cities_clean(cits):
  output = ''
  cities = sorted(cits, key=lambda k: k['city'])
  for cit_pair in cities:
    day_str = ' days'
    if cit_pair['count'] == 1:
      day_str = ' day'
    output = output + cit_pair['city'] + ': ' + str(cit_pair['count']) + day_str + '<br>'
  output = output[:-4]
  return output


def cities_clean(cits):
  output = []
  cities = sorted(cits, key=lambda k: k['city'])
  for cit_pair in cities:
    day_str = ' days'
    if cit_pair['count'] == 1:
      day_str = ' day'
    output.append(''.join([cit_pair['city'], ': ', str(cit_pair['count']), day_str]))
    # output = output + cit_pair['city'] + ': ' + str(cit_pair['count']) + day_str + '<br>'
  # output = output[:-4]
  return output


def city_nums():
  """
  Get a dictionary of Backpage city names mapped to their 'legend' value.

  Returns:
    dictionary of Backpage city names mapped to their numeric value
  """
  city_nums = {}
  first_row = 1
  num = 0
  fname = pkg_resources.resource_filename(__name__, 'resources/Distance_Matrix.csv')
  with open(fname, 'rU') as csvfile:
    reader = csv.reader(csvfile, delimiter = ',')
    for row in reader:
      if first_row == 1:
        first_row = 0
      else:
        city_nums[row[0]] = num
        num = num + 1

  return city_nums


def countries():
  countries = []
  with open('dataFiles/country_names.csv', 'rU') as csvfile:
    reader = csv.reader(csvfile, delimiter = ',')
    for row in reader:
      countries.append(row[0].lower().replace(' ', ''))
  return set(countries)


def date_clean(date, dashboard_style=False):
  """
  Clean the numerical date value in order to present it.

  Args:
    boo: numerical date (20160205)
  Returns:
    Stringified version of the input date ("2016-02-05")
  """
  if dashboard_style:
    dt = str(date)
    out = dt[4:6] + '/' + dt[6:] + '/' + dt[:4]
  else:
    dt = str(date)
    out = dt[:4] + '-' + dt[4:6] + '-' + dt[6:]
  return out


# Get the formatted date for a day N days go
def date_n_days_ago(n, clean=False):
  day = datetime.strptime((datetime.today() - timedelta(n)).date().isoformat().replace('-', ' '), '%Y %m %d')
  if not clean:
    return int(str(day).replace('-', '')[:8])
  else:
    return str(day)[:10]


def date_range(start, end, boo):
  """
  Return list of dates within a specified range, inclusive.

  Args:
    start: earliest date to include, String ("2015-11-25")
    end: latest date to include, String ("2015-12-01")
    boo: if true, output list contains Numbers (20151230); if false, list contains Strings ("2015-12-30")
  Returns:
    list of either Numbers or Strings
  """
  earliest = datetime.strptime(start.replace('-', ' '), '%Y %m %d')
  latest = datetime.strptime(end.replace('-', ' '), '%Y %m %d')
  num_days = (latest - earliest).days + 1
  all_days = [latest - timedelta(days=x) for x in range(num_days)]
  all_days.reverse()

  output = []

  if boo:
    # Return as Integer, yyyymmdd
    for d in all_days:
      output.append(int(str(d).replace('-', '')[:8]))
  else:
    # Return as String, yyyy-mm-dd
    for d in all_days:
      output.append(str(d)[:10])
  return output


def dicts_equal(d1, d2):
  """
    Perform a deep comparison of two dictionaries
    Handles:
      - Primitives
      - Nested dicts
      - Lists of primitives
  """

  # check for different sizes
  if len(d1) != len(d2):
    return False
  # check for different keys
  for k in d1:
    if k not in d2:
      return False
  for k in d2:
    if k not in d1:
      return False

  # compare each element in dict
  for k in d1:
    if type(d1[k]) != type(d2[k]):
      # different value types
      return False

    # lists
    elif isinstance(d1[k], list):
      if not (sorted(d1[k]) == sorted(d2[k])):
        return False

    # nested dicts
    elif isinstance(d1[k], dict):
      if not dicts_equal(d1[k], d2[k]):
        return False

    # primitives
    else:
      if d1[k] != d2[k]:
        return False

  return True



def distances():
  """
  Get all distances between all cities in miles (matrix-style). 

  Returns:
    dictionary of Backpage city names mapped to a list of distances, one for every other city
  """
  distances = {}
  # Instantiate a matrix of distances (in miles) between all cities
  num = 0
  top_row = 1
  fname = pkg_resources.resource_filename(__name__, 'resources/Distance_Matrix.csv')
  with open(fname, 'rU') as csvfile:
    reader = csv.reader(csvfile, delimiter = ',')
    for row in reader:
      if top_row == 1:
        top_row = 0
      else:
        # Map each city to a list of distances to all other cities.
        vals = []
        for item in row[1:]:
          if not item:
            continue
          try:
            vals.append(int(float(item)))
          except ValueError:
            print 'Invalid data type for row {} with value {}, column value: {}'.format(num, row[0], item)
        distances[num] = vals
        num += 1

  return distances


def ethnicity_nums():
  # Map ethnicities to a numeric value
  ethn_legend = {}
  ethn_list = ['white_non_hispanic', 'hispanic_latino', 'american_indian', 'asian', 'midEast_nAfrican', 
        'african_american', 'subsaharan_african', 'multiracial']
  for e in range(len(ethn_list)):
    ethn_legend[ethn_list[e]] = e+1

  return ethn_legend


def ethnicities_clean():
  """ Get dictionary of unformatted ethnicity types mapped to clean corresponding ethnicity strings """
  eths_clean = {}

  fname = pkg_resources.resource_filename(__name__, 'resources/Ethnicity_Groups.csv')
  with open(fname, 'rU') as csvfile:
    reader = csv.reader(csvfile, delimiter = ',')
    first = []
    for row in reader:
      if first:
        for i in range(len(first)):
          if first[i] and row[i]:
            eths_clean[first[i]] = row[i]
        first = []
      else:
        first = deepcopy(row)
  return eths_clean


def filter_text(body, title):
  output = body.replace('\\r', '').replace('\\n', '').replace('&amp;', '')
  # Remove HTML tags
  output = re.sub(r'<.*?>', ' ', output)
  # Remove successive spaces
  output = re.sub(r' +', ' ', output)

  output = title + ' ~~~ ' + output

  return output


def formal_cities(reverse=False):
  """
  Get a dictionary that maps all Backpage city names to their presentable, formal names.

  Returns:
    dictionary of Backpage city names mapped to formal city names
  """
  output = {}
  fname = pkg_resources.resource_filename(__name__, 'resources/Formal_City_Name_Pairs.csv')
  with open(fname, 'rU') as csvfile:
    reader = csv.reader(csvfile, delimiter = ',')
    for row in reader:
      if not reverse:
        # Informal to formal
        output[row[0]] = row[1]
      else:
        # Formal to informal
        output[row[1]] = row[0]
  return output


def get_indicator_keys(value, reverse=False):

  indicator_dict = {
    'new to the country': {'key': 1, 'clean': 'New to the Country'},
    '<<natasha/svetlana': {'key': 2, 'clean': 'Natasha Svetlana'},
    'young': {'key': 3, 'clean': 'Young'},
    'just hit 18': {'key': 5, 'clean': 'Just Turned 18'},
    'just turned 18': {'key': 5, 'clean': 'Just Turned 18'},
    'fresh meat': {'key': 6, 'clean': 'Fresh Meat'},
    'virgin': {'key': 7, 'clean': 'Virgin'},
    'foreign': {'key': 8, 'clean': 'Foreign'},
    'fresh off the boat': {'key': 9, 'clean': 'Fresh off the Boat'},
    'easy sex': {'key': 10, 'clean': 'Easy Sex'},
    'come chat with me': {'key': 11, 'clean': 'Come Chat with Me'},
    'massage/nuru': {'key': 12, 'clean': 'Massage'},
    'escort agency': {'key': 13, 'clean': 'Escort Agency'},
    '<<<has website>>>': {'key': 14, 'clean': 'Has a Website'},
    '<<<college world series/baseball>>>': {'key': 15, 'clean': 'Baseball College World Series'},
    '<<address>>': {'key': 16, 'clean': 'Address'},
    '<<high turnover of girls>>': {'key': 17, 'clean': 'High turnover of workers'},
    'brand new': {'key': 18, 'clean': 'Brand New'},
    'college': {'key': 19, 'clean': 'College'},
    'teen': {'key': 20, 'clean': 'Teen'},
    'high school': {'key': 21, 'clean': 'High School'},
    'daddy\'s little girl': {'key': 22, 'clean': 'Daddy\'s Little Girl'},
    'fresh': {'key': 23, 'clean': 'Fresh'},
    '100% <eth>': {'key': 24, 'clean': '100% <ethnicity>'},
    'speaks ## language(s)': {'key': 25, 'clean': 'Speaks N Languages'},
    'new to the country/us': {'key': 26, 'clean': 'New to the Country/US'},
    'massage parlor': {'key': 27, 'clean': 'Massage Parlor'},
    'come see us at': {'key': 28, 'clean': 'Come see us at...'},
    'in/outcall': {'key': 29, 'clean': 'Incall/Outcall'},
    '420/party friendly': {'key': 30, 'clean': 'Drugs'},
    'no men under 35': {'key': 31, 'clean': 'No Men Under 35'},
    'no hispanics/indians': {'key': 32, 'clean': 'No Hispanics/Indians'},
    'available/open 24/7': {'key': 33, 'clean': 'Available/Open 24/7'},
    '<<ad is in Spanish>>': {'key': 34, 'clean': 'Ad is in Spanish'},
    'underage': {'key': 35, 'clean': 'Underage'},
    'no_indians': {'key': 36, 'clean': 'No Indians'},
    'no_hispanics': {'key': 37, 'clean': 'No Hispanics'},
    'incall': {'key': 38, 'clean': 'Incall'},
    'outcall': {'key': 39, 'clean': 'Outcall'}
  }

  if value not in indicator_dict.values()[0]:
    print 'Invalid data type for indicators. Valid data types are "key" or "clean".'
    return

  output_dict = {}
  for ind in indicator_dict:
    if not reverse:
      # Normal mapping
      output_dict[ind] = indicator_dict[ind][value]
    else:
      # Reverse the mapping
      output_dict[indicator_dict[ind][value]] = ind

  return output_dict


def get_lats():
  """
  Get a dictionary that maps Backpage city names to their respective latitudes.

  Returns:
    dictionary that maps city names (Strings) to latitudes (Floats)
  """
  lats = {}
  fname = pkg_resources.resource_filename(__name__, 'resources/Latitudes-Longitudes.csv')
  with open(fname, 'rb') as csvfile:
    # Read latitude/longitude coordinates
    reader = csv.reader(csvfile, delimiter = ',')
    for row in reader:
      word = row[0].lower()
      word = re.sub(' ', '', word)
      lats[word] = float(row[1])

  return lats


def get_longs():
  """
  Get a dictionary that maps Backpage city names to their respective longitudes.

  Returns:
    dictionary that maps city names (Strings) to longitudes (Floats)
  """
  longs = {}
  fname = pkg_resources.resource_filename(__name__, 'resources/Latitudes-Longitudes.csv')
  with open(fname, 'rb') as csvfile:
    # Read latitude/longitude coordinates
    reader = csv.reader(csvfile, delimiter = ',')
    for row in reader:
      word = row[0].lower()
      word = re.sub(' ', '', word)
      longs[word] = float(row[2])
  return longs


# Get set of names from the DB
def get_names_set(names_coll, data_type):

  names_src = list(names_coll.find({'type': data_type}))[0]['data']

  if isinstance(names_src, dict):
    names = {}
  else:
    names = []

  for name in names_src:
    word = name.lower()
    if isinstance(names_src, dict):
      # Name alt mapped to grammar-checked name
      names[word] = names_src[name]
    else:
      # List of raw names
      if ' ' in word:
        names.append(word.replace(' ', ''))
      names.append(word)

  # Dict of name alt mapped to grammar-checked name
  if isinstance(names, dict):
    return names

  # Set of raw, unique names
  names = set(names)
  return names


def get_regions():
  """
  Get a dictionary of state names mapped to their respective region numeric values.

  New England -> 0
  Mid Atlantic -> 1
  Midwest East -> 2
  Midwest West -> 3
  Southeast -> 4
  Southwest -> 5
  Mountain West -> 6
  Pacific -> 7
  Alaska -> 8
  Hawaii -> 9

  Returns:
    dictionary of state names mapped to region numbers
  """
  new_england = ['Connecticut', 'Maine', 'Massachusetts', 'New Hampshire', 'Rhode Island', 'Vermont']
  mid_atlantic = ['New Jersey', 'New York', 'Pennsylvania', 'Delaware', 'Maryland', 'District of Columbia']
  midwest_east = ['Illinois', 'Indiana', 'Michigan', 'Ohio', 'Wisconsin']
  midwest_west = ['Iowa', 'Kansas', 'Minnesota', 'Missouri', 'Nebraska', 'North Dakota', 'South Dakota']
  southeast = ['Florida', 'Georgia', 'South Carolina', 'Virginia', 'Alabama', 'Kentucky', 'Mississippi', 'Tennessee', 'Arkansas', 'Louisiana', 'West Virginia', 'North Carolina']
  southwest = ['Texas', 'Oklahoma', 'New Mexico', 'Arizona']
  mtn_west = ['Montana', 'Idaho', 'Wyoming', 'Colorado', 'Nevada', 'Utah']
  pacific = ['Washington', 'Oregon', 'California']
  alaska = ['Alaska']
  hawaii = ['Hawaii']

  regions = []
  regions.append(new_england)
  regions.append(mid_atlantic)
  regions.append(midwest_east)
  regions.append(midwest_west)
  regions.append(southeast)
  regions.append(southwest)
  regions.append(mtn_west)
  regions.append(pacific)
  regions.append(alaska)
  regions.append(hawaii)

  # Map each state to its region number
  output = {}
  for i in range(len(regions)):
    states = regions[i]
    for j in range(len(states)):
      output[states[j]] = i
  return output


def populations():
  """
  Get a dictionary of Backpage city names mapped to their citizen populations.

  Returns:
    dictionary of Backpage city names mapped to their populations (integers)
  """
  city_pops = {}
  fname = pkg_resources.resource_filename(__name__, 'resources/CityPops.csv')
  with open(fname, 'rU') as csvfile:
    reader = csv.reader(csvfile, delimiter = ',')
    for row in reader:
      city_pops[row[0]] = int(row[1])
  return city_pops


def post_id_backpage_groups():
  """
  Get a dictionary of Backpage city names mapped to their posting ID group (ex: groups['buffalo']: 'upstateny')

  Returns:
    dictionary of Backpage city names mapped to their posting ID group
  """
  city_bp_groups = {}
  with open('dataFiles/city_bp_groups.csv', 'rU') as csvfile:
    reader = csv.reader(csvfile, delimiter = ',')
    for row in reader:
      city_bp_groups[row[0]] = row[1]

  return city_bp_groups


def region_populations():
  regions = get_regions()
  state_pops = state_populations()

  region_pops = {}
  for state in regions:
    if regions[state] in region_pops:
      region_pops[regions[state]] += state_pops[state]
    else:
      region_pops[regions[state]] = state_pops[state]
  return region_pops


def state_names():
  """ Get the set of all US state names """
  
  names = set()
  fname = pkg_resources.resource_filename(__name__, 'resources/States.csv')
  with open(fname, 'rU') as csvfile:
    reader = csv.reader(csvfile, delimiter = ',')
    for row in reader:
      names.add(row[0])
  return names


def state_nums():
  """
  Get a dictionary of state names mapped to their 'legend' value.

  Returns:
    dictionary of state names mapped to their numeric value
  """
  st_nums = {}
  fname = pkg_resources.resource_filename(__name__, 'resources/States.csv')
  with open(fname, 'rU') as csvfile:
    reader = csv.reader(csvfile, delimiter = ',')
    i = 0
    for row in reader:
      st_nums[row[0]] = i
      i = i + 1
  return st_nums


def state_populations():
  city_pops = populations()
  state_cities = states_cities()

  state_pops = {}
  for state in state_cities:
    total_pop = 0
    for city in state_cities[state]:
      total_pop += city_pops[city]
    state_pops[state] = total_pop
  return state_pops


def states():
  """
  Get a dictionary of Backpage city names mapped to their respective states.

  Returns:
    dictionary of Backpage city names mapped to their states
  """
  states = {}
  fname = pkg_resources.resource_filename(__name__, 'resources/City_State_Pairs.csv')
  with open(fname, 'rU') as csvfile:
    reader = csv.reader(csvfile, delimiter = ',')
    for row in reader:
      states[row[0]] = row[1]

  return states


# Map state name strings to lists of city strings (cities that are within that state)
def states_cities():
  sts = states()
  out = {}
  for city in sts.keys():
    st = sts[city]
    if st in out:
      out[st].append(city)
    else:
      l = []
      l.append(city)
      out[st] = l
  return out


def today(boo):
  """
  Return today's date as either a String or a Number, as specified by the User.

  Args:
    boo: if true, function returns Number (20151230); if false, returns String ("2015-12-30")
  Returns:
    either a Number or a string, dependent upon the user's input
  """
  tod = datetime.strptime(datetime.today().date().isoformat().replace('-', ' '), '%Y %m %d')
  if boo:
    return int(str(tod).replace('-', '')[:8])
  else:
    return str(tod)[:10]


def weekday_map():
  wmap = {
    0: 'Monday',
    1: 'Tuesday',
    2: 'Wednesday',
    3: 'Thursday',
    4: 'Friday',
    5: 'Saturday',
    6: 'Sunday'
  }
  return wmap


def yesterday(boo):
  """
  Return yesterday's date as either a String or a Number, as specified by the User.

  Args:
    boo: if true, function returns Number (20151230); if false, returns String ("2015-12-30")
  Returns:
    either a Number or a string, dependent upon the user's input
  """
  if boo:
    return date_n_days_ago(1)
  else:
    return date_n_days_ago(1, clean=True)
