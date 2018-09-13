from copy import deepcopy
import json
import re

import parser
from parser_helpers import get_clean_loc_parts, get_clean_parts, get_location_section
from unicode_decoder import decode_unicode


def parse_single_ad(ad, global_names, common_words, args={}):
  """
    An example extraction of a Backpage ad, with the following parameters:
      ad -> A dict representing an ad that is scraped as such:
        ad = items.BackpageScrapeItem(
          backpage_id=response.url.split('.')[0].split('/')[2].encode('utf-8'),
          date = str(self.static_now)[:10],
          posted_age = response.xpath("//p[@class='metaInfoDisplay']/text()").extract()[0].encode('utf-8'),
          posted_date = response.xpath("//div[@class='adInfo']/text()").extract()[0].encode('utf-8'),
          posted_title = response.xpath("//div[@id='postingTitle']//h1/text()").extract()[0].encode('utf-8'),
          posting_body= response.xpath("//div[@class='postingBody']").extract()[0].encode('utf-8'),
          text = response.body,
          url=response.url
        )
  """

  multiple_phones = False if 'multiple_phones' not in args else args['multiple_phones']

  item = {}

  # Backpage category
  ## 1 --> FemaleEscorts
  ## 2 --> BodyRubs
  ## 3 --> Dating section (after 1/9/17 Backpage shutdown of FemaleEscorts and BodyRubs)
  ## 4 --> TherapeuticMassage section (1/23/17 partial begin date, 1/24/17 full begin date)
  if 'therapeuticmassage' in ad['url'].split('.backpage.com/')[1].lower():
    item['category'] = 4
  else:
    item['category'] = 3
  
  # parse age
  if item['category'] == 4:
    item['age'] = -1
  else:
    item['age'] = int(re.sub(r'\D', '', ad['posted_age'][14:]))

  # Get rid of any posted age under 10 and over 60. Assign to -1 if it is invalid
  if item['age'] < 10 or item['age'] > 60:
    item['age'] = -1

  ageless_title = re.sub(r' {1,2}- {1,2}\d\d\Z', '', ad['posted_title'])
  ad_text = json.dumps(ad['text'].lower())

  # Get filtered, decomposed list of body + title parts.
  # 'Parts' are separated only when there is a newline (\n) character in the ad.
  decoded_body = decode_unicode(ad['posting_body'], replace_boo=False)[1]
  decoded_title = decode_unicode(ageless_title, replace_boo=False)[1]
  parts = get_clean_parts(decoded_body, decoded_title)
  loc_section = get_location_section(ad_text)
  loc_parts = get_clean_loc_parts(loc_section, is_location=True)

  all_parts = {
    'body': parts,
    'loc': [','.join(loc_parts)]
  }
  
  item['time'] = parser.parse_time(repr(ad['posted_date']))

  item['post_id'] = parser.parse_posting_id(ad_text, ad['city'])

  # Find/remove phone number
  ret_data = parser.parse_phone(all_parts['body'], allow_multiple=multiple_phones)
  item['phone'] = ret_data[0]
  all_parts['body'] = ret_data[1]
  if multiple_phones:
    # parse phone(s) from the URL 
    url = ad['url'].replace('http://', '')
    url = re.sub(r'\w+\.backpage\.com/\w+/', '', url)
    url = url.split('/')
    ret_data = parser.parse_phone(url, allow_multiple=multiple_phones)
    if ret_data[0]:
      item['phone'].extend(ret_data[0])
      item['phone'] = list(set(item['phone']))

  # Find/remove "No blacks allowed"
  ret_data = parser.parse_no_blacks(all_parts['body'])
  item['no_blacks'] = ret_data[0]
  all_parts['body'] = ret_data[1]

  # Find/remove ethnicity(s)
  ret_data = parser.parse_ethnicity(all_parts['body'])
  item['ethnicity'] = ret_data[0]
  all_parts['body'] = ret_data[1]

  # Find/remove "trucker friendly"
  ret_data = parser.parse_truckers(all_parts['body'])
  tf = ret_data[0]
  if not tf:
    tf = parser.parse_truckers(all_parts['loc'])[0]
  item['trucker'] = tf
  all_parts['body'] = ret_data[1]

  # Find names
  item['name'] = parser.parse_name(all_parts['body'], global_names, common_words)

  # Indicators that may indicate a HT victim. Still in early stages
  item['indicators'] = parser.parse_indicators(all_parts['body'], item['ethnicity'])

  # Parse whether or not the ad lists 'military' friendly
  item['military'] = parser.parse_military_friendly(all_parts['body'])

  return item


if __name__ == '__main__':

  # load regular and common names
  with open('dataFiles/names.json', 'rU') as reader:
    data = json.load(reader)
    global_names = deepcopy(data['regular'])
    common_words = deepcopy(data['common'])

  args = {
    'multiple_phones': True
  }

  ADS_LIST = []
  for ad in ADS_LIST:
    parsed_ad = parse_single_ad(ad, global_names, common_words, args)
