from typing import List
from etk.extractor import Extractor, InputType
from etk.etk_extraction import Extraction
from etk.extractors.date_parser import DateParser
import re


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
        date_list = self.extract_date_str(text)
        for date_str in date_list:
            date = date_parser.extract(date_str['value'], settings)
            if date and len(date):
                extracted_date = Extraction(str(date[0].value), self.name, start_char=date_str['start'], end_char=date_str['start']+len(date_str['value']))
                # should be better if re-consider the construction of provenance:
                extracted_date._provenance['original_date_str'] = date_str['value']
                res.append(extracted_date)
        return res

    def extract_date_str(self, text: str) -> List[dict]:
        """
        TODO: To extract the date str, by apply the regex here, actually we have already known which is \
            Month, Date and Year. So I think the date str can be converted to iso date directly.
            The dateparser module is not so necessary if we have to extract date str from a long text first.
        """
        """ extract sub-strings in the text that is possible to be a date """
        mdy = re.compile(r"\b((?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?|1[0-2]|0?[1-9])[./\-_ ,]{0,2}(?:3[01]|[0-2]?[0-9](?:th|rd|st|nd)?)[./\-_ ,]{0,2}(?:(?:[12][0-9])?\d{2}))\b", re.I)
        dmy = re.compile(r"\b((?:3[01]|[0-2]?[0-9](?:th|rd|st|nd)?)[./\-_ ,]{0,2}(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?|1[0-2]|0?[1-9])[./\-_ ,]{0,2}(?:(?:[12][0-9])?\d{2}))\b", re.I)
        ymd = re.compile(r"\b((?:(?:[12][0-9])?\d{2})[./\-_ ,]{0,2}(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?|1[0-2]|0?[1-9])[./\-_ ,]{0,2}(?:3[01]|[0-2]?[0-9](?:th|rd|st|nd)?))\b", re.I)
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

