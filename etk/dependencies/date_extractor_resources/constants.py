singleton_regex = {
    '%Y': r'([1-2][0-9][0-9][0-9])',  # year in four digits
    '%y': r'([6-9][0-9]|[0-3][0-9])',  # year in two digits
    '%B': r'(January|February|March|April|May|June|July|August|September|October|November|December|'    # month
          r'enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre|'  # spainish
          r'januari|februari|mars|april|maj|juni|juli|augusti|september|oktober|november|december)',    # swedish
    '%b': r'(Jan\.?|Feb\.?|Mar\.?|Apr\.?|Jun\.?|Jul\.?|Aug\.?|Sep(?:t?)\.?|Oct\.?|Nov\.?|Dec\.?|'
          r'enero|feb\.?|marzo|abr\.?|mayo|jun\.?|jul\.?|agosto|sept\.?|set\.?|oct\.?|nov\.?|dic\.?)',  # month abbr.
    '%m': r'(1[0-2]|0?[1-9])',  # month in two digits: 01-12 or just 0-12
    '%d': r'(3[0-1]|[1-2][0-9]|0?[1-9])',  # day in two digits: 01-31 or juest 1-31
    'd_suffix': r'(?:st|nd|rd|th)',
    '%A': r'(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday|'
          r'lunes|martes|miércoles|jueves|viernes|sábado|domingo|'
          r'Måndag|Tisdag|Onsdag|Torsdag|Fredag|Lördag|Söndag)',  # weekdays
    '%a': r'(Mon\.?|Tue\.?|Wed\.?|Thu(?:r(?:s?)?)?\.?|Fri\.?|Sat\.?|Sun\.?|L|M|X|J|V|S|D)',  # weekdays abbr.
    '%H': r'(2[0-3]|1[0-9]|0?[0-9])',  # hour in 24-hours in two digits: 00-23 or just 0-23
    '%I': r'(1[0-2]|0?[1-9])',  # hour in 12-hour in two digits: 01-12 or just 1-12
    '%p': r'((?: ?)AM(?:\.?)|(?: ?)PM(?:\.?))',  # am/pm markers
    '%M': r'([0-5][0-9])',  # minute in two digits: 00-59
    '%S': r'([0-5][0-9](?:\.[0-9]+)?)',  # second in two digits: 00-59 or with fractions like 12.355
    '%Z': r'(ACDT|ACST|ACT|ACT|ACWST|ADT|AEDT|AEST|AFT|AKDT|AKST|AMST|AMT|AMT|ART|AST|AST|AWST|AZOST|AZOT|AZT|'
          r'BDT|BIOT|BIT|BOT|BRST|BRT|BST|BST|BST|BTT|CAT|CCT|CDT|CDT|CEST|CET|CHADT|CHAST|CHOT|CHOST|CHST|CHUT|'
          r'CIST|CIT|CKT|CLST|CLT|COST|COT|CST|CST|CST|CT|CVT|CWST|CXT|DAVT|DDUT|DFT|'
          r'EASST|EAST|EAT|ECT|ECT|EDT|EEST|EET|EGST|EGT|EIT|EST|FET|FJT|FKST|FKT|FNT|'
          r'GALT|GAMT|GET|GFT|GILT|GIT|GMT|GST|GST|GYT|HDT|HAEC|HST|HKT|HMT|HOVST|HOVT|'
          r'ICT|IDT|IOT|IRDT|IRKT|IRST|IST|IST|IST|JST|KGT|KOST|KRAT|KST|LHST|LHST|LINT|'
          r'MAGT|MART|MAWT|MDT|MET|MEST|MHT|MIST|MIT|MMT|MSK|MST|MST|MUT|MVT|MYT|NCT|NDT|NFT|NPT|NST|NT|NUT|NZDT|NZST|'
          r'OMST|ORAT|PDT|PET|PETT|PGT|PHOT|PHT|PKT|PMDT|PMST|PONT|PST|PST|PYST|PYT|RET|ROTT|'
          r'SAKT|SAMT|SAST|SBT|SCT|SDT|SGT|SLST|SRET|SRT|SST|SST|SYOT|TAHT|THA|TFT|TJT|TKT|TLT|TMT|TRT|TOT|TVT|'
          r'ULAST|ULAT|USZ1|UTC|UYST|UYT|UZT|VET|VLAT|VOLT|VOST|VUT|WAKT|WAST|WAT|WEST|WET|WIT|WST|YAKT|YEKT)',
    # timezone abbr. list
    # reference: https://en.wikipedia.org/wiki/List_of_time_zone_abbreviations
    '%z': r'((?:UTC|GMT)(?: ?[\+\-] ?(?:(?:1[0-4]|0?[0-9])(?::?(?:00|30|45))?))?|'
          r'[+-][01][0-3](?:00|30|45)|[\+\-](?:1[0-3]|0[0-9])(?:00|30|45))',
    # timezone like 'UTC', 'UTC + 8:30', 'GMT+1130', '+0600'
    # reference: https://en.wikipedia.org/wiki/Time_zone
    # 'splitters': r'(?:[,/ \.\-]? ?)',
    'splitters': r'(?:,? of |,? del |,? de |,? el |\. |, |-|/| ?)',
    'y_prefix': r'in |at |by |year ',
    'w_prefix': r'on ',
}

possible_illegal = r'^\b(?:[0-9]{3}[-/ ,\.] ?[0-9]{4}|[0-9]{3}|[0-9]{5}|[0-9]{2}[-/ ,\.] ?[0-9]{4}|' \
                   r'[0-9]{4}[-/ ,\.] ?[0-9]{3}|[0-9]{2}[-/ ,\.] ?[0-9]{1}|[0-9]{1}[-/ ,\.] ?[0-9]{2}|' \
                   r'[0-9]{2}|[0-9]{1,2} ?[0-9]{1,2} ?[0-9]{1,2}|[0-9]{1,2}[- ,.:][0-9]{1,2}|[0-9]{1}|' \
                   r'[0-9]{1}[-/ ,\.][0-9]{1})\b$'

units = {
    'Y': ['%Y', '%y'],
    'M': ['%B', '%b', '%m'],
    'D': ['%d', ['%d', 'd_suffix']],
    'W': ['%A', '%a'],
    'HOUR': ['%I', '%H'],
    'MIN': '%M',
    'SEC': '%S',
    'MARK': '%p',
    'TZ': ['%Z', '%z'],
    'SINGLE_YEAR': [['y_prefix', '%Y']],
    'SINGLE_WEEK': ['%A', ['w_prefix', '%a']],
    'SINGLE_MONTH': ['%B', '%b']
}

day_of_week_to_number = {
    'monday': 0,
    'tuesday': 1,
    'wednesday': 2,
    'thursday': 3,
    'friday': 4,
    'saturday': 5,
    'sunday': 6
}

foreign_to_english = {
    # spanish
    'enero': 'January',
    'febrero': 'February',
    'marzo': 'March',
    'abril': 'April',
    'mayo': 'May',
    'junio': 'June',
    'julio': 'July',
    'agosto': 'August',
    'septiembre': 'September',
    'octubre': 'October',
    'noviembre': 'November',
    'diciembre': 'December',
    'feb': 'Feb',
    'abr': 'Apr',
    'set': 'Sept',
    'dic': 'Dec',
    'lunes': 'Monday',
    'martes': 'Tuesday',
    'miércoles': 'Wednesday',
    'jueves': 'Thursday',
    'viernes': 'Friday',
    'sábado': 'Saturday',
    'domingo': 'Sunday',
    'L': 'Mon',
    'M': 'Tue',
    'X': 'Wed',
    'J': 'Thu',
    'V': 'Fri',
    'S': 'Sat',
    'D': 'Sun',

    # swedish
    'januari': 'January',
    'februari': 'February',
    'mars': 'March',
    'april': 'April',
    'maj': 'May',
    'juni': 'June',
    'juli': 'July',
    'augusti': 'August',
    'september': 'September',
    'oktober': 'October',
    'november': 'November',
    'december': 'December',
    'måndag': 'Monday',
    'tisdag': 'Tuesday',
    'onsdag': 'Wednesday',
    'torsdag': 'Thursday',
    'fredag': 'Friday',
    'lördag': 'Saturday',
    'söndag': 'Sunday'
}

language_date_order = {
    'es': 'DMY',
    'en': 'MDY'
}

# relative date:
# before, after, last, next, ago, later, in
# number, half, quarter, several?, some?,
# weeks, years, days, months, hours, minutes, seconds
# now, , yesterday, tomorrow
directions = {
    'last': '-',
    'next': '+',
    'before': '-',
    'after': '+',
    'in': '+',
    'ago': '-',
    'later': '+'
}
num_to_digit = {
    'a': '1',
    'an': '1',
    'one': '1',
    'two': '2',
    'three': '3',
    'four': '4',
    'five': '5',
    'six': '6',
    'seven': '7',
    'eight': '8',
    'nine': '9',
    'ten': '10'
}
spacy_rules = {
  "field_name": "relative_date",
  "rules": [
    {
      "dependencies": [],
      "description": "",
      "identifier": "direction_number_unit",
      "is_active": "true",
      "output_format": "",
      "pattern": [
        {
          "capitalization": [],
          "contain_digit": "false",
          "is_in_output": "true",
          "is_in_vocabulary": "false",
          "is_out_of_vocabulary": "false",
          "is_required": "true",
          "length": [],
          "match_all_forms": "true",
          "maximum": "",
          "minimum": "",
          "numbers": [],
          "part_of_speech": [],
          "prefix": "",
          "shapes": [
          ],
          "suffix": "",
          "token": ["in", "after", "before"
          ],
          "type": "word"
        },
        {
          "capitalization": [],
          "contain_digit": "false",
          "is_in_output": "true",
          "is_in_vocabulary": "false",
          "is_out_of_vocabulary": "false",
          "is_required": "true",
          "length": [],
          "match_all_forms": "true",
          "maximum": "",
          "minimum": "",
          "numbers": [],
          "part_of_speech": [],
          "prefix": "",
          "shapes": [
          ],
          "suffix": "",
          "token": ["a", "an", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten"],
          "type": "word"
        },
        {
          "capitalization": [],
          "contain_digit": "false",
          "is_in_output": "true",
          "is_in_vocabulary": "false",
          "is_out_of_vocabulary": "false",
          "is_required": "true",
          "length": [],
          "match_all_forms": "true",
          "maximum": "",
          "minimum": "",
          "numbers": [],
          "part_of_speech": [],
          "prefix": "",
          "shapes": [
          ],
          "suffix": "",
          "token": ["weeks", "years", "days", "months", "hours", "minutes", "seconds",
                    "week", "year", "day", "month", "hour", "minute", "second"],
          "type": "word"
        }
      ],
      "polarity": "true"
    },
    {
      "dependencies": [],
      "description": "",
      "identifier": "number_unit_direction",
      "is_active": "true",
      "output_format": "",
      "pattern": [
        {
          "capitalization": [],
          "contain_digit": "false",
          "is_in_output": "true",
          "is_in_vocabulary": "false",
          "is_out_of_vocabulary": "false",
          "is_required": "true",
          "length": [],
          "match_all_forms": "true",
          "maximum": "",
          "minimum": "",
          "numbers": [],
          "part_of_speech": [],
          "prefix": "",
          "shapes": [
          ],
          "suffix": "",
          "token": ["a", "an", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten"],
          "type": "word"
        },
        {
          "capitalization": [],
          "contain_digit": "false",
          "is_in_output": "true",
          "is_in_vocabulary": "false",
          "is_out_of_vocabulary": "false",
          "is_required": "true",
          "length": [],
          "match_all_forms": "true",
          "maximum": "",
          "minimum": "",
          "numbers": [],
          "part_of_speech": [],
          "prefix": "",
          "shapes": [
          ],
          "suffix": "",
          "token": ["weeks", "years", "days", "months", "hours", "minutes", "seconds",
                    "week", "year", "day", "month", "hour", "minute", "second"],
          "type": "word"
        },
        {
          "capitalization": [],
          "contain_digit": "false",
          "is_in_output": "true",
          "is_in_vocabulary": "false",
          "is_out_of_vocabulary": "false",
          "is_required": "true",
          "length": [],
          "match_all_forms": "true",
          "maximum": "",
          "minimum": "",
          "numbers": [],
          "part_of_speech": [],
          "prefix": "",
          "shapes": [
          ],
          "suffix": "",
          "token": ["before", "ago", "later", "after"],
          "type": "word"
        }
      ],
      "polarity": "true"
    },
    {
      "dependencies": [],
      "description": "",
      "identifier": "direction_digit_unit",
      "is_active": "true",
      "output_format": "",
      "pattern": [
        {
          "capitalization": [],
          "contain_digit": "false",
          "is_in_output": "true",
          "is_in_vocabulary": "false",
          "is_out_of_vocabulary": "false",
          "is_required": "true",
          "length": [],
          "match_all_forms": "true",
          "maximum": "",
          "minimum": "",
          "numbers": [],
          "part_of_speech": [],
          "prefix": "",
          "shapes": [
          ],
          "suffix": "",
          "token": ["in", "after", "before"
          ],
          "type": "word"
        },
        {
          "capitalization": [],
          "contain_digit": "true",
          "is_in_output": "true",
          "is_in_vocabulary": "false",
          "is_out_of_vocabulary": "false",
          "is_required": "true",
          "length": [],
          "match_all_forms": "true",
          "maximum": "0",
          "minimum": "",
          "numbers": [],
          "part_of_speech": [],
          "prefix": "",
          "shapes": [
          ],
          "suffix": "",
          "token": [
          ],
          "type": "number"
        },
        {
          "capitalization": [],
          "contain_digit": "false",
          "is_in_output": "true",
          "is_in_vocabulary": "false",
          "is_out_of_vocabulary": "false",
          "is_required": "true",
          "length": [],
          "match_all_forms": "true",
          "maximum": "",
          "minimum": "",
          "numbers": [],
          "part_of_speech": [],
          "prefix": "",
          "shapes": [
          ],
          "suffix": "",
          "token": ["weeks", "years", "days", "months", "hours", "minutes", "seconds",
                    "week", "year", "day", "month", "hour", "minute", "second"],
          "type": "word"
        }
      ],
      "polarity": "true"
    },
    {
      "dependencies": [],
      "description": "",
      "identifier": "digit_unit_direction",
      "is_active": "true",
      "output_format": "",
      "pattern": [
        {
          "capitalization": [],
          "contain_digit": "true",
          "is_in_output": "true",
          "is_in_vocabulary": "false",
          "is_out_of_vocabulary": "false",
          "is_required": "true",
          "length": [],
          "match_all_forms": "true",
          "maximum": "0",
          "minimum": "",
          "numbers": [],
          "part_of_speech": [],
          "prefix": "",
          "shapes": [
          ],
          "suffix": "",
          "token": [
          ],
          "type": "number"
        },
        {
          "capitalization": [],
          "contain_digit": "false",
          "is_in_output": "true",
          "is_in_vocabulary": "false",
          "is_out_of_vocabulary": "false",
          "is_required": "true",
          "length": [],
          "match_all_forms": "true",
          "maximum": "",
          "minimum": "",
          "numbers": [],
          "part_of_speech": [],
          "prefix": "",
          "shapes": [
          ],
          "suffix": "",
          "token": ["weeks", "years", "days", "months", "hours", "minutes", "seconds",
                    "week", "year", "day", "month", "hour", "minute", "second"],
          "type": "word"
        },
        {
          "capitalization": [],
          "contain_digit": "false",
          "is_in_output": "true",
          "is_in_vocabulary": "false",
          "is_out_of_vocabulary": "false",
          "is_required": "true",
          "length": [],
          "match_all_forms": "true",
          "maximum": "",
          "minimum": "",
          "numbers": [],
          "part_of_speech": [],
          "prefix": "",
          "shapes": [
          ],
          "suffix": "",
          "token": ["before", "ago", "later", "after"],
          "type": "word"
        }
      ],
      "polarity": "true"
    },
    {
      "dependencies": [],
      "description": "",
      "identifier": "direction_unit",
      "is_active": "true",
      "output_format": "",
      "pattern": [
        {
          "capitalization": [],
          "contain_digit": "false",
          "is_in_output": "true",
          "is_in_vocabulary": "false",
          "is_out_of_vocabulary": "false",
          "is_required": "true",
          "length": [],
          "match_all_forms": "true",
          "maximum": "",
          "minimum": "",
          "numbers": [],
          "part_of_speech": [],
          "prefix": "",
          "shapes": [
          ],
          "suffix": "",
          "token": ["last", "next"
          ],
          "type": "word"
        },
        {
          "capitalization": [],
          "contain_digit": "false",
          "is_in_output": "true",
          "is_in_vocabulary": "false",
          "is_out_of_vocabulary": "false",
          "is_required": "true",
          "length": [],
          "match_all_forms": "true",
          "maximum": "",
          "minimum": "",
          "numbers": [],
          "part_of_speech": [],
          "prefix": "",
          "shapes": [
          ],
          "suffix": "",
          "token": ["week", "year", "day", "month", "hour", "minute", "second"],
          "type": "word"
        }
      ],
      "polarity": "true"
    },
    {
      "dependencies": [],
      "description": "",
      "identifier": "the_day",
      "is_active": "true",
      "output_format": "",
      "pattern": [
    {
          "capitalization": [],
          "contain_digit": "false",
          "is_in_output": "true",
          "is_in_vocabulary": "false",
          "is_out_of_vocabulary": "false",
          "is_required": "false",
          "length": [],
          "match_all_forms": "true",
          "maximum": "",
          "minimum": "",
          "numbers": [],
          "part_of_speech": [],
          "prefix": "",
          "shapes": [
          ],
          "suffix": "",
          "token": ["the"],
          "type": "word"
        },
        {
          "capitalization": [],
          "contain_digit": "false",
          "is_in_output": "true",
          "is_in_vocabulary": "false",
          "is_out_of_vocabulary": "false",
          "is_required": "false",
          "length": [],
          "match_all_forms": "true",
          "maximum": "",
          "minimum": "",
          "numbers": [],
          "part_of_speech": [],
          "prefix": "",
          "shapes": [
          ],
          "suffix": "",
          "token": ["day"],
          "type": "word"
        },
        {
          "capitalization": [],
          "contain_digit": "false",
          "is_in_output": "true",
          "is_in_vocabulary": "false",
          "is_out_of_vocabulary": "false",
          "is_required": "false",
          "length": [],
          "match_all_forms": "true",
          "maximum": "",
          "minimum": "",
          "numbers": [],
          "part_of_speech": [],
          "prefix": "",
          "shapes": [
          ],
          "suffix": "",
          "token": ["before", "after"],
          "type": "word"
        },
        {
          "capitalization": [],
          "contain_digit": "false",
          "is_in_output": "true",
          "is_in_vocabulary": "false",
          "is_out_of_vocabulary": "false",
          "is_required": "true",
          "length": [],
          "match_all_forms": "true",
          "maximum": "",
          "minimum": "",
          "numbers": [],
          "part_of_speech": [],
          "prefix": "",
          "shapes": [
          ],
          "suffix": "",
          "token": ["yesterday", "tomorrow"],
          "type": "word"
        },
      ],
      "polarity": "true"
    }
  ]
}