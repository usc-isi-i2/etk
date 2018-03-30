import unittest, datetime
from etk.extractors.date_extractor import DateExtractor, DateResolution
from etk.etk import ETK

# with open('etk/unit_tests/ground_truth/date_ground_truth.txt', 'r') as f:
#     sample = f.read()

with open('ground_truth/date_ground_truth.txt', 'r') as f:
    sample = f.read()

de = DateExtractor(ETK(), 'unit_test')
relative = "I will graduate in 5 days. My classmates traveled to Hawaii last month. I hope that " \
           "I could have a vocation after two weeks. Last year I was very busy. yesterday, The day after Tomorrow"
less_info = "May June JULY march dec"
span_dates = "el 29 de febrero de 1996 vs lunes, el 24 de junio, 2013 vs 3 de octubre de 2017"
wired_formats = "2010@3@29  Jul.$15$17     Thur 2018, Apr. 5  and Mon 2017, Mar 6th"

class TestDateExtractor(unittest.TestCase):
    def test_default(self) -> None:
        extractions = de.extract(sample)
        results = [e.value for e in extractions]

        expected = ['2017-02-12', '2017-02-12', '2015-10-02', '2015-10-02', '2015-10-02', '2015-10-02', '2015-10-02',
                    '2015-10-02', '2015-10-02', '2015-10-02', '2015-10-02', '2015-10-02', '2015-10-02', '2015-10-02',
                    '2015-10-01', '2015-10-01', '2015-10-01', '2015-10-01', '2015-10-02', '2015-10-02', '2015-10-02',
                    '2015-10-02', '2015-10-03', '2015-10-03', '2015-10-03', '2015-10-03', '2015-10-04', '2015-10-04',
                    '2015-10-04', '2015-10-04', '2018-03-10', '2018-03-10', '2018-03-10', '2018-03-10', '2018-03-10',
                    '2018-03-10', '2018-03-10', '2018-03-10', '2010-03-01', '2018-03-10', '2018-03-10', '2018-03-10',
                    '2018-03-10', '2018-03-10', '2018-03-10', '2018-03-10', '2018-03-10', '2018-03-10', '2018-03-10',
                    '2018-03-10', '2018-03-10', '2018-03-10', '2018-03-10', '2018-03-10', '2018-03-10', '2018-03-10',
                    '2018-03-10', '2018-03-10', '2018-03-10', '2018-03-10', '2018-03-10', '2018-03-10', '2014-09-04',
                    '2012-09-09', '2009-12-20', '2009-12-20', '2013-12-04', '2014-02-01', '2013-12-06', '2014-04-16',
                    '2014-04-16', '2016-06-27', '2016-07-04', '2016-12-01', '2006-03-08', '2006-03-08', '2006-03-08',
                    '2006-03-08', '2006-03-08', '2006-03-08', '2006-08-03', '2006-08-03', '2006-08-03', '2006-08-03',
                    '2006-08-03', '2006-08-03', '2006-08-03', '2006-08-03', '2006-08-03', '2006-08-03', '2003-06-08',
                    '2006-08-03', '2003-06-08', '2006-08-03', '2006-08-03', '2006-08-03', '2006-08-03', '2006-08-03',
                    '2006-08-03', '2006-08-03', '2006-08-03', '2006-08-03', '2006-08-01', '2020-06-01', '2006-08-03',
                    '2006-08-03', '2006-08-03', '2018-07-01', '2018-06-01', '2019-03-01', '2017-12-01', '1998-01-09',
                    '2020-01-07', '2020-02-05', '2018-03-21', '2018-03-29']

        self.assertEqual(results, expected)

    def test_range(self) -> None:
        ignore_before = datetime.datetime(2015, 10, 1)
        ignore_after = datetime.datetime(2015, 10, 30)
        extractions = de.extract(sample,
                                 ignore_dates_before=ignore_before,
                                 ignore_dates_after=ignore_after,
                                 )
        results = [e.value for e in extractions]

        expected = ['2015-10-02', '2015-10-02', '2015-10-02', '2015-10-02', '2015-10-02', '2015-10-02', '2015-10-02',
                    '2015-10-02', '2015-10-02', '2015-10-02', '2015-10-02', '2015-10-02', '2015-10-01', '2015-10-01',
                    '2015-10-01', '2015-10-01', '2015-10-02', '2015-10-02', '2015-10-02', '2015-10-02', '2015-10-03',
                    '2015-10-03', '2015-10-03', '2015-10-03', '2015-10-04', '2015-10-04', '2015-10-04', '2015-10-04']

        self.assertEqual(results, expected)


    # def test_range(self) -> None:
    #     de.extract(sample,
    #                extract_first_date_only=False,  # first valid
    #                additional_formats=['%Y@%m@%d', '%a %Y, %b %d'],
    #
    #                use_default_formats=True,
    #
    #                # ignore_dates_before: datetime.datetime = None,
    #                ignore_dates_before=ignore_before,
    #
    #                # ignore_dates_after: datetime.datetime = None,
    #                ignore_dates_after=ignore_after,
    #
    #                detect_relative_dates=False,
    #
    #                relative_base=relative_base,
    #
    #                # preferred_date_order: str = "MDY",  # used for interpreting ambiguous dates that are missing parts
    #                preferred_date_order="DMY",
    #
    #                prefer_language_date_order=True,
    #
    #                # timezone: str = None,  # default is local timezone.
    #                # timezone='GMT',
    #
    #                # to_timezone: str = None,  # when not specified, not timezone conversion is done.
    #                # to_timezone='UTC',
    #
    #                # return_as_timezone_aware: bool = True
    #                return_as_timezone_aware=False,
    #
    #                # prefer_day_of_month: str = "first",  # can be "current", "first", "last".
    #                prefer_day_of_month='first',
    #
    #                # prefer_dates_from: str = "current"  # can be "future", "future", "past".)
    #                prefer_dates_from='future',
    #                )