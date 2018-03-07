from typing import List
from etk.extractor import Extractor, InputType
from etk.etk_extraction import Extraction
import dateparser
import datetime
import re


class DateParser(object):
    """
    Parse a date in string to an ISO formatted date object
    """
    def __init__(self, ignore_future_dates: bool=True, ignore_past_years: int=40) -> None:
        self.ignore_future_dates = ignore_future_dates
        self.ignore_past_years = ignore_past_years

    def parse_date(self, str_date: str, settings: dict=None) -> datetime.datetime or None:
        # TODO: May not need to use dateparser, as the date str is already recognized when extract from text
        """

        Args:
            str_date (): a date in strin
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
            see more on https://github.com/scrapinghub/dateparser/blob/master/docs/usage.rst

        Returns: a datetime.datetime object (or None if the string is not a date)

        """
        customized_settings = settings if settings else {'STRICT_PARSING': True}
        try:
            if len(str_date) > 100:
                return None
            str_date = str_date[:20] if len(str_date) > 20 else str_date
            str_date = str_date.replace('\r', '')
            str_date = str_date.replace('\n', '')
            str_date = str_date.replace('<', '')
            str_date = str_date.replace('>', '')
            parsed_date = dateparser.parse(str_date, settings=customized_settings)
            if parsed_date:
                parsed_year = parsed_date.year
                current_year = datetime.datetime.now().year
                if current_year - self.ignore_past_years > parsed_year:
                    return None
                if self.ignore_future_dates:
                    return parsed_date if datetime.datetime.now() >= parsed_date else None
            return self.convert_to_iso_format(parsed_date)
        except Exception as e:
            print('Exception: {}, failed to parse {} as date'.format(e, str_date))
            return None

    @staticmethod
    def convert_to_iso_format(date: datetime.datetime):
        """  """
        try:
            if date:
                dt = date.replace(minute=0, hour=0, second=0, microsecond=0)
                return dt.isoformat()
        except Exception as e:
            print('Exception: {}, failed to convert {} to isoformat '.format(e, date))
            return None
        return None


class DateExtractor(Extractor):
    def __init__(self, extractor_name: str) -> None:
        Extractor.__init__(self,
                           input_type=InputType.TEXT,
                           category="data extractor",
                           name=extractor_name)

    def extract(self, text: str=None, ignore_future_dates: bool=True, ignore_past_years: int=20,
                settings: dict=None) -> List[Extraction]:
        """
        go through the text to find some sub strings for date,
        when meet a substring for date, parse it by DateParser,
        and wrap the result in an Extraction,
        return the list of Extractions
        """

        res = list()
        date_parser = DateParser(ignore_future_dates, ignore_past_years)
        for date_str in self.extract_date_str(text):
            date = date_parser.parse_date(date_str['value'], settings)
            if date:
                extracted_date = Extraction(str(date), self.name, start_char=date_str['start'], end_char=date_str['start']+len(date_str['value']))
                # should be better if re-consider the construction of provenance:
                extracted_date._provenance['original_date_str'] = date_str['value']
                res.append(extracted_date)
        return res

    def extract_date_str(self, text: str) -> List[dict]:
        """ extract sub-strings in the text that is possible to be a date """
        mdy = re.compile("((?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?|1[0-2]|0?[1-9])[./\-_ ,]{0,2}(?:3[01]|[0-2]?[0-9](?:th|rd|st|nd)?)[./\-_ ,]{0,2}(?:(?:[12][0-9])?\d{2}))", re.I)
        dmy = re.compile("((?:3[01]|[0-2]?[0-9](?:th|rd|st|nd)?)[./\-_ ,]{0,2}(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?|1[0-2]|0?[1-9])[./\-_ ,]{0,2}(?:(?:[12][0-9])?\d{2}))", re.I)
        ymd = re.compile("((?:(?:[12][0-9])?\d{2})[./\-_ ,]{0,2}(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?|1[0-2]|0?[1-9])[./\-_ ,]{0,2}(?:3[01]|[0-2]?[0-9](?:th|rd|st|nd)?))", re.I)
        return self.union_results([mdy.finditer(text), dmy.finditer(text), ymd.finditer(text)])

    def union_results(self, results: List[iter]) -> List[dict]:
        """ union date str extracted by different regex together, keep the longer one when overlapped """
        same_start = {}
        same_end = {}
        for result in results:
            for x in result:
                value = re.sub(r'(\b\d{3}\b)', '', x.group()).strip(r'./\-_ ,')
                length = len(value)
                if length < 6:
                    continue
                start = x.span()[0]
                end = x.span()[1]
                if (start in same_start and len(same_start[start]['value']) > length) or (end in same_end and len(same_end[end]['value']) > length):
                    continue

                if start not in same_start and end not in same_end:
                    same_start[start] = {'end': end, 'value': value}
                    same_end[end] = {'start': start, 'value': value}
                    continue

                if end in same_end and start not in same_start:
                    same_start[start] = {'end': end, 'value': value}
                    if len(same_end[end]['value']) < length:
                        same_end[end]['value'] = value
                        del same_start[same_end[end]['start']]
                        same_end[end]['start'] = start

                if start in same_start and end not in same_end:
                    same_end[end] = {'start': start, 'value': value}
                    if len(same_start[start]['value']) < length:
                        same_start[start]['value'] = value
                        del same_end[same_start[start]['end']]
                        same_start[start]['end'] = end

        return sorted(list(same_end.values()), key=lambda k: k['start'])

