from typing import List
from etk.extractor import Extractor, InputType
from etk.etk_extraction import Extraction
import dateparser
import datetime

class DateParser(Extractor):
    """
    Parse a date in string to an ISO formatted date object
    """
    def __init__(self, ignore_future_dates: bool=True, ignore_past_years: int=40) -> None:
        self.ignore_future_dates = ignore_future_dates
        self.ignore_past_years = ignore_past_years
        Extractor.__init__(self,
                           input_type=InputType.TEXT,
                           category="data extractor",
                           name="date parser")

    def extract(self, str_date: str, settings: dict=None) -> List[Extraction]:
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
        ori_str = str_date
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
                parsed_year = parsed_date.year
                current_year = datetime.datetime.now().year
                if current_year - self.ignore_past_years > parsed_year:
                    return list()
                if self.ignore_future_dates and datetime.datetime.now() < parsed_date:
                    return list()
            extracted_date = Extraction(str(self.convert_to_iso_format(parsed_date)), self.name)
            # should be better if re-consider the construction of provenance:
            extracted_date._provenance['original_date_str'] = ori_str
            return [extracted_date]
        except Exception as e:
            print('Exception: {}, failed to parse {} as date'.format(e, str_date))
            return list()

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
