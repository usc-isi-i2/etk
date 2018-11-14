from typing import List, Dict
from dateutil import parser
from datetime import timedelta
import datetime
from etk.extractors.date_extractor import DateExtractor, DateResolution
import sys

de = DateExtractor(extractor_name='date_extractor')

"""
Detect simple granularity types in a sequence of dates.
"""


class GranularityDetector:
    granularity = {
        "second": timedelta(seconds=1),
        "minute": timedelta(minutes=1),
        "hour": timedelta(hours=1),
        "day": timedelta(hours=24),
        "week": timedelta(days=7),
        "month": timedelta(days=30),
        "quarter": timedelta(days=120),
        "year": timedelta(days=365)
    }

    @staticmethod
    def get_parsed_date(my_date):
        if isinstance(my_date, datetime.datetime):
            return my_date
        elif isinstance(my_date, datetime.date):
            return datetime.datetime(my_date.year, my_date.month, my_date.day)
        elif isinstance(my_date, str):
            try:
                extraction = de.extract(my_date,
                                        date_value_resolution=DateResolution.SECOND,
                                        use_default_formats=True,
                                        additional_formats=['%Y'])
                iso_date = extraction[0].value
                parsed_date = parser.parse(iso_date) if iso_date else None

                return parsed_date
            except:
                return None
        return None

    @staticmethod
    def get_granularity(dates: List, return_best=True, min_confidence=0.5) -> Dict or str:
        try:
            if len(dates) == 1:
                return "unknown"

            granularity_confidence = dict()
            parsed_dates = [GranularityDetector.get_parsed_date(date) for date in dates]

            for g in GranularityDetector.granularity:
                time_delta = GranularityDetector.granularity[g]
                confidence = 0.0

                for i in range(1, len(parsed_dates)):
                    if parsed_dates[i] and parsed_dates[i - 1]:
                        time_diff = abs(parsed_dates[i] - parsed_dates[i - 1])
                        error = abs(time_diff - time_delta) / (
                                    time_diff + time_delta)  # Is there a better measure for error?
                        confidence += 1 - error

                confidence /= (len(parsed_dates) - 1)
                granularity_confidence[g] = confidence

            if return_best:
                most_confident = max(granularity_confidence.keys(), key=lambda key: granularity_confidence[key])
                if granularity_confidence[most_confident] >= min_confidence:
                    return most_confident
                else:
                    return "unknown"
            else:
                return granularity_confidence

        except:
            print("Unexpected error:", sys.exc_info()[0])

            if return_best:
                return "unknown"
            else:
                return dict()
