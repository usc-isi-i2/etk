# reference: http://www.gnu.org/software/libc/manual/html_node/Formatting-Calendar-Time.html
# symbol references: http://strftime.org/
singleton_regex = {
    '%Y': r'([1-2][0-9][0-9][0-9])',  # year in four digits
    '%y': r'([6-9][0-9]|[0-3][0-9])',  # year in two digits
    '%B': r'(January|February|March|April|May|June|July|August|September|October|November|December)',  # month
    '%b': r'(Jan\.?|Feb\.?|Mar\.?|Apr\.?|Jun\.?|Jul\.?|Aug\.?|Sep(?:t?)\.?|Oct\.?|Nov\.?|Dec\.?)',  # month abbr.
    '%m': r'(1[0-2]|0[1-9])',  # month in two digits: 01-12
    '%-m': r'(1[0-2]|[1-9])',  # month in number without prefix 0: 1-12
    '%d': r'(3[0-1]|[1-2][0-9]|0[1-9])',  # day in two digits: 01-31
    '%-d': r'(3[0-1]|[1-2][0-9]|[1-9])',  # day in number without prefix 0: 1-31
    'd_suffix': r'(?:st|nd|rd|th)',
    '%A': r'(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)',  # weekdays
    '%a': r'(Mon\.?|Tue\.?|Wed\.?|Th(?:u(?:r(?:s?)?)?)\.?|Fri\.?|Sat\.?|Sun\.?)',  # weekdays abbr.
    '%H': r'(2[0-3]|[0-1][0-9])',  # hour in 24-hours in two digits: 00-23
    '%-H': r'(2[0-3]|1[0-9]|[0-9])',  # hour in 24-hour in number without prefix 0: 0-23
    '%I': r'(1[0-2]|0[1-9])',  # hour in 12-hour in two digits: 01-12
    '%-I': r'(1[0-2]|[1-9])',  # hour in 12-hour in number without prefix 0: 1-12
    '%p': r'((?: ?)AM(?:\.?)|(?: ?)PM(?:\.?))',  # am/pm markers
    '%M': r'([0-5][0-9])',  # minute in two digits: 00-59
    '%S': r'([0-5][0-9])',  # second in two digits: 00-59
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
    'splitters': r'(?:[,/ \.\-]? ?)'
}

units = {
    'Y': ['%Y', '%y'],
    'M': ['%B', '%b', '%m', '%-m'],
    'D': ['%d', '%-d', ['%d', 'd_suffix'], ['%-d', 'd_suffix']],
    'W': ['%A', '%a'],
    'HOUR': ['%I', '%-I', '%H', '%-H'],
    'MIN': '%M',
    'SEC': '%S',
    'MARK': '%p',
    'TZ': ['%Z', '%z']
}


def generate_regex_for_a_unit(key_list):
    if isinstance(key_list, list):
        if not key_list or len(key_list) == 0:
            return {'res': r'', 'pattern_list': []}
        pattern_list = []
        regex_list = []
        for key in key_list:
            if isinstance(key, list):
                regex = singleton_regex[key[0]]
                if key[0][0] == '%':
                    pattern_list.append(key[0])
                for sub_key in key[1:]:
                    regex = regex + singleton_regex[sub_key]
                    if sub_key[0] == '%':
                        pattern_list.append(sub_key)
                regex = regex
                regex_list.append(regex)
            else:
                regex_list.append(singleton_regex[key])
                if key[0] == '%':
                    pattern_list.append(key)
        if not regex_list or len(regex_list) == 0:
            return {'res': r'', 'pattern_list': []}
        res = r'(?:' + regex_list[0]
        for r in regex_list[1:]:
            res = res + '|' + r
        res = res + ')'
        return {'res': res, 'pattern_list': pattern_list}
    else:
        return {'res': r'(?:' + singleton_regex[key_list] + r')',
                'pattern_list': [key_list] if key_list[0] == '%' else []}

def generate_all_regexes_by_singletons():
    all = {}
    pattern = []
    for k in units.keys():
        unit = generate_regex_for_a_unit(units[k])
        # print(unit)
        all[k] = unit
    # print(pattern)
    s = singleton_regex['splitters']

    time = r'(?:' + all['HOUR']['res'] + r': ?' + all['MIN']['res'] + r'(?:: ?' + all['SEC']['res'] + r')?(?:' + all[
        'MARK']['res'] + ')?(?:' + s + all['TZ']['res'] + r')?)'
    st = r'(?:' + singleton_regex['splitters'] + r'|T)?'
    time_reg = r'(?:T?' + st + time + r'Z?)?'

    week_reg_post = r'(?:' + s + all['W']['res'] + r')?'
    week_reg_pre = r'(?:' + all['W']['res'] + s + r')?'

    day_reg_post = r'(?:' + s + all['D']['res'] + r')?'
    day_reg_pre = r'(?:' + all['D']['res'] + s + r')?'

    pl = 'pattern_list'
    time_pattern = all['HOUR'][pl] + all['MIN'][pl] + all['SEC'][pl] + all['MARK'][pl] + all['TZ'][pl]

    return ({
                'MDY': r'(?<=\b)(?:(?:' + week_reg_pre + all['M']['res'] + day_reg_post + s + all['Y'][
                    'res'] + week_reg_post + r')' + time_reg + r')(?=\b)',
                'DMY': r'(?<=\b)(?:(?:' + week_reg_pre + day_reg_pre + all['M']['res'] + s + all['Y'][
                    'res'] + week_reg_post + r')' + time_reg + r')(?=\b)',
                'YMD': r'(?<=\b)(?:(?:' + week_reg_pre + all['Y']['res'] + s + all['M'][
                    'res'] + day_reg_post + week_reg_post + r')' + time_reg + r')(?=\b)'
            }, {
                'MDY': all['W'][pl] + all['M'][pl] + all['D'][pl] + all['Y'][pl] + all['W'][pl] + time_pattern,
                'DMY': all['W'][pl] + all['D'][pl] + all['M'][pl] + all['Y'][pl] + all['W'][pl] + time_pattern,
                'YMD': all['W'][pl] + all['Y'][pl] + all['M'][pl] + all['D'][pl] + all['W'][pl] + time_pattern
            })


temp = generate_all_regexes_by_singletons()
# print(json.dumps(temp, indent=2))
final_regex = temp[0]
symbol_list = temp[1]