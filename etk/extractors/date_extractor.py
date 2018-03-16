from typing import List
from enum import Enum, auto
from etk.extractor import Extractor, InputType
from etk.etk_extraction import Extraction
import datetime, re, dateparser


class DateResolution(Enum):
    """

    """
    SECOND = auto()
    MINUTE = auto()
    HOUR = auto()
    DAY = auto()
    MONTH = auto()
    YEAR = auto()


class DateExtractor(Extractor):
    def __init__(self, extractor_name: str='date extractor') -> None:
        Extractor.__init__(self,
                           input_type=InputType.TEXT,
                           category="data extractor",
                           name=extractor_name)

        # reference: http://www.gnu.org/software/libc/manual/html_node/Formatting-Calendar-Time.html
        self.singleton_regex = {
            '%Y': r'[0-9][0-9][0-9][0-9]',  # year in four digits
            '%y': r'[6-9][0-9]|[0-3][0-9]',  # year in two digits
            '%B': r'\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\b', # month
            '%b': r'\b(?:Jan\.?|Feb\.?|Mar\.?|Apr\.?|Jun\.?|Jul\.?|Aug\.?|Sep(?:t?)\.?|Oct\.?|Nov\.?|Dec\.?)\b',    # month abbr.
            '%m': r'1[0-2]|0[1-9]',  # month in two digits: 01-12
            '%f': r'1[0-2]|[1-9]',  # month in number without prefix 0: 1-12
            '%d': r'3[0-1]|[1-2][0-9]|0[1-9]',  # day in two digits: 01-31
            '%e': r'3[0-1]|[1-2][0-9]|[1-9]',  # day in number without prefix 0: 1-31
            '%d_suffix': r'st|nd|rd|th',
            '%A': r'\b(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\b',  # weekdays
            '%a': r'\b(?:Mon\.?|Tue\.?|Wed\.?|Th(?:u(?:r(?:s?)?)?)\.?|Fri\.?|Sat\.?|Sun\.?)\b',  # weekdays abbr.
            '%H': r'2[0-3]|[0-1][0-9]',  # hour in 24-hours in two digits: 00-23
            '%k': r'2[0-3]|1[0-9]|[0-9]',  # hour in 24-hour in number without prefix 0: 0-23
            '%I': r'1[0-2]|0[1-9]',  # hour in 12-hour in two digits: 01-12
            '%l': r'1[0-2]|[1-9]',  # hour in 12-hour in number without prefix 0: 1-12
            '%p': r' ?AM\.?| ?PM\.?',  # am/pm markers
            '%M': r'[0-5][0-9]',  # minute in two digits: 00-59
            '%S': r'[0-5][0-9]',  # second in two digits: 00-59
            '%Z': r'ACDT|ACST|ACT|ACT|ACWST|ADT|AEDT|AEST|AFT|AKDT|AKST|AMST|AMT|AMT|ART|AST|AST|AWST|AZOST|AZOT|AZT|'
                  r'BDT|BIOT|BIT|BOT|BRST|BRT|BST|BST|BST|BTT|CAT|CCT|CDT|CDT|CEST|CET|CHADT|CHAST|CHOT|CHOST|CHST|CHUT|'
                  r'CIST|CIT|CKT|CLST|CLT|COST|COT|CST|CST|CST|CT|CVT|CWST|CXT|DAVT|DDUT|DFT|'
                  r'EASST|EAST|EAT|ECT|ECT|EDT|EEST|EET|EGST|EGT|EIT|EST|FET|FJT|FKST|FKT|FNT|'
                  r'GALT|GAMT|GET|GFT|GILT|GIT|GMT|GST|GST|GYT|HDT|HAEC|HST|HKT|HMT|HOVST|HOVT|'
                  r'ICT|IDT|IOT|IRDT|IRKT|IRST|IST|IST|IST|JST|KGT|KOST|KRAT|KST|LHST|LHST|LINT|'
                  r'MAGT|MART|MAWT|MDT|MET|MEST|MHT|MIST|MIT|MMT|MSK|MST|MST|MUT|MVT|MYT|NCT|NDT|NFT|NPT|NST|NT|NUT|NZDT|NZST|'
                  r'OMST|ORAT|PDT|PET|PETT|PGT|PHOT|PHT|PKT|PMDT|PMST|PONT|PST|PST|PYST|PYT|RET|ROTT|'
                  r'SAKT|SAMT|SAST|SBT|SCT|SDT|SGT|SLST|SRET|SRT|SST|SST|SYOT|TAHT|THA|TFT|TJT|TKT|TLT|TMT|TRT|TOT|TVT|'
                  r'ULAST|ULAT|USZ1|UTC|UYST|UYT|UZT|VET|VLAT|VOLT|VOST|VUT|WAKT|WAST|WAT|WEST|WET|WIT|WST|YAKT|YEKT',
            # timezone abbr. list
            # reference: https://en.wikipedia.org/wiki/List_of_time_zone_abbreviations
            '%z': r'(?:UTC|GMT)(?: ?[\+\-] ?(?:(?:1[0-4]|0?[0-9])(?::?(?:00|30|45))?))?|'
                  r'[+-][01][0-3](?:00|30|45)|[\+\-](?:1[0-3]|0[0-9])(?:00|30|45)',
            # timezone like 'UTC', 'UTC + 8:30', 'GMT+1130', '+0600'
            # reference: https://en.wikipedia.org/wiki/Time_zone
            '%splitters': r'[,/\s\.\-]'
        }

        self.units = {
            'Y': ['%Y', '%y'],
            'M': ['%B', '%b', '%m', '%f'],
            'D': ['%d', '%e', ['%d', '%d_suffix'], ['%e', '%d_suffix']],
            'W': ['%A', '%a'],
            'HOUR': ['%I', '%l', '%H', '%k'],
            'MIN': '%M',
            'SEC': '%S',
            'MARK': '%p',
            'TZ': ['%Z', '%z']
        }

        self.final_regex = self.generate_all_regexes_by_singletons()

    def extract(self, text: str = None,
                extract_first_date_only: bool = False,
                # useful for news stories where the first date is typically the publication date.
                date_formats: List[str] = list(),  # when specified, only extract the given formats.
                ignore_dates_before: datetime.datetime = None,
                ignore_dates_after: datetime.datetime = None,
                detect_relative_dates: bool = True,
                relative_base: datetime.datetime = None,
                preferred_date_order: str = "MDY",  # used for interpreting ambiguous dates that are missing parts
                prefer_language_date_order: bool = True,
                timezone: str = None,  # default is local timezone.
                to_timezone: str = None,  # when not specified, not timezone conversion is done.
                return_as_timezone_aware: bool = True,  # when false don't do timezime conversions.
                prefer_day_of_month: str = "first",  # can be "current", "first", "last".
                prefer_dates_from: str = "current"  # can be "current", "future", "past".
                ) -> List[Extraction]:
        results = [list(re.finditer(d, text)) for d in self.final_regex]
        res = []
        all_results = []
        for x in results:
            all_results = all_results + x
        if not all_results or len(all_results) == 0:
            return list()
        all_results.sort(key=lambda k: k.start())
        cur_max = all_results[0]
        for x in all_results[1:]:
            if cur_max.end() <= x.start():
                res.append(self.wrap(cur_max))
                cur_max = x
            else:
                if len(x.group()) > len(cur_max.group()):
                    cur_max = x
        res.append(self.wrap(cur_max))
        # TODO: do not append if parse_date returns None
        return res

    @staticmethod
    def parse_date(str_date: str,
                   date_formats: List[str] = list(),  # when specified, only extract the given formats.
                   ignore_dates_before: datetime.datetime = None,
                   ignore_dates_after: datetime.datetime = None,
                   detect_relative_dates: bool = True,
                   relative_base: datetime.datetime = None,
                   preferred_date_order: str = "MDY",  # used for interpreting ambiguous dates that are missing parts
                   prefer_language_date_order: bool = True,
                   timezone: str = None,  # default is local timezone.
                   to_timezone: str = None,  # when not specified, not timezone conversion is done.
                   return_as_timezone_aware: bool = True,  # when false don't do timezime conversions.
                   prefer_day_of_month: str = "first",  # can be "current", "first", "last".
                   prefer_dates_from: str = "current"  # can be "current", "future", "past".
                   ) -> datetime.datetime or None:
        """
        settings (): settings when parse the date:
            {
                'DATE_ORDER': 'MDY',    # default to be 'MDY', shuffled Y, M, D representing Year, Month, Date
                'STRICT_PARSING': True,
                'FUZZY': True,
                'PREFER_DAY_OF_MONTH': 'current',   # default to be 'current'; can be 'first' or 'last' instead;\
                    specify the date when the date is missing
                'PREFER_DATES_FROM': 'current_period',  # default to be 'current_period'; can be 'future', \
                    or 'past' instead; specify the date when the date is missing
                'RELATIVE_BASE': datetime.datetime(2020, 1, 1),  # default to be current date and time
                'SKIP_TOKENS_PARSER': ['t']    # default to be ['t']; a list of tokens to discard while detecting language
            }
        Args:
            str_date:
            date_formats:
            ignore_dates_before:
            ignore_dates_after:
            detect_relative_dates:
            relative_base:
            preferred_date_order:
            prefer_language_date_order:
            timezone:
            to_timezone:
            return_as_timezone_aware:
            prefer_day_of_month:
            prefer_dates_from:

        Returns:

        """
        # TODO: convert the params to settings for dateparser
        customized_settings = {
            'STRICT_PARSING': False
        }
        try:
            if len(str_date) > 100:
                return list()
            str_date = str_date[:20] if len(str_date) > 20 else str_date
            str_date = str_date.replace('\r', '')
            str_date = str_date.replace('\n', '')
            str_date = str_date.replace('<', '')
            str_date = str_date.replace('>', '')
            parsed_date = dateparser.parse(str_date, settings=customized_settings)
            if parsed_date:
                # TODO: deal with the constraints like 'ignore_dates_after', 'ignore_dates_before'
                return parsed_date
        except Exception as e:
            print('Exception: {}, failed to parse {} as date'.format(e, str_date))
            return list()

    @staticmethod
    def convert_to_iso_format(date: datetime.datetime, resolution: DateResolution = DateResolution.DAY):
        """  """
        # TODO: deal with the date resolution
        try:
            if date:
                dt = date.replace(minute=0, hour=0, second=0, microsecond=0)
                return dt.isoformat()
        except Exception as e:
            print('Exception: {}, failed to convert {} to isoformat '.format(e, date))
            return None
        return None

    def generate_all_regexes_by_singletons(self):
        all = {}
        for k in self.units.keys():
            all[k] = self.generate_regex_for_a_unit(self.units[k])
        s = self.singleton_regex['%splitters'] + r'? ?'
        time = r'(' + all['HOUR'] + r': ?' + all['MIN'] + r'(?:: ?' + all['SEC'] + r')?(?:' + all[
            'MARK'] + ')?(?:' + s + all['TZ'] + r')?)'
        st = r'(?:' + self.singleton_regex['%splitters'] + r' ?|T)?'
        time_reg = r'(?:T?' + st + time + r'Z?)?'
        week_reg = r'(?:' + s + all['W'] + r')?'
        return [
            # 'MDY':
            r'\b(((?:' + all['W'] + s + r')?' + all['M'] + s + all['D'] + r'?' + s + all[
                'Y'] + week_reg + r')' + time_reg + r')\b',
            # 'DMY':
            r'\b(((?:' + all['W'] + s + r')?' + all['D'] + r'?' + s + all['M'] + s + all[
                'Y'] + week_reg + r')' + time_reg + r')\b',
            # 'YMD':
            r'\b(((?:' + all['W'] + s + r')?' + all['Y'] + s + all['M'] + s + all[
                'D'] + r'?' + week_reg + r')' + time_reg + r')\b',
        ]

    def wrap(self, match):
        value = str(self.convert_to_iso_format(self.parse_date(match.group())))
        e = Extraction(value=value, start_char=match.start(), end_char=match.end(), extractor_name=self.name)
        # match.groups() contains the separated date and time
        # TODO: need a better way to do this
        e._provenance['original_string'] = match.group()
        return e

    def generate_regex_for_a_unit(self, key_list):
        if isinstance(key_list, list):
            if not key_list or len(key_list) == 0:
                return list()
            regex_list = []
            for key in key_list:
                if isinstance(key, list):
                    regex = r'(?:' + self.singleton_regex[key[0]]
                    for sub_key in key[1:]:
                        regex = regex + ')(?:' + self.singleton_regex[sub_key]
                    regex = regex + ')'
                    regex_list.append(regex)
                else:
                    regex_list.append('(?:'+self.singleton_regex[key]+')')
            if not regex_list or len(regex_list) == 0:
                return r''
            res = r'(?:' + regex_list[0]
            for r in regex_list[1:]:
                res = res + '|' + r
            res = res + ')'
            return res
        else:
            return r'(?:' + self.singleton_regex[key_list] + r')'

# with open('date_etl/date_ground_truth.txt', 'r') as f:
#     text = f.read()
# aaa = DateExtractor(extractor_name='test')
# bbb = aaa.extract(text)
# cnt=0
# for x in bbb:
#     if x.value == 'None':
#         print(x._provenance['original_string'])
#         cnt += 1
#     print(x.value)
#
# print(len(bbb), cnt)