import unittest, datetime
from etk.extractors.date_extractor import DateExtractor, DateResolution
from etk.etk import ETK

with open('etk/unit_tests/ground_truth/date_ground_truth.txt', 'r') as f:
    sample = f.read()

de = DateExtractor(ETK(), 'unit_test_date')

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
                    '2020-01-07', '2020-02-05', '2018-03-22', '2018-03-30']

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

    def test_additional_formats(self) -> None:
        text = '2018@3@25  July 29 in 2018, 4/3@2018  2009-10-23 Jun 27 2017'
        formats = ['%Y@%m@%d', '%B %d in %Y', '%m/%d@%Y']

        extractions_with_default = de.extract(text, additional_formats=formats, use_default_formats=True )
        extractions_without_default = de.extract(text, additional_formats=formats, use_default_formats=False )

        results_with_default = [e.value for e in extractions_with_default]
        results_without_default = [e.value for e in extractions_without_default]

        expected_with_default = ['2018-03-25', '2018-07-29', '2018-04-03', '2009-10-23', '2017-06-27']
        expected_without_default = ['2018-03-25', '2018-07-29', '2018-04-03']

        self.assertEqual(results_with_default, expected_with_default)
        self.assertEqual(results_without_default, expected_without_default)

    def test_relative_date(self) -> None:
        text = '5 days ago, in two months, last year, yesterday, the day after tomorrow  2009-10-23 Jun 27 2017'

        extractions_with_base = de.extract(text, detect_relative_dates=True, relative_base=datetime.datetime(2018, 1, 1))
        extractions_base_tody = de.extract(text, detect_relative_dates=True)

        results_with_base = [e.value for e in extractions_with_base]
        results_base_today = [e.value for e in extractions_base_tody]

        expected_with_base = ['2009-10-23', '2017-06-27', '2017-12-27', '2018-03-01', '2017-01-01', '2017-12-31', '2018-01-03']
        expected_base_today = ['2009-10-23', '2017-06-27', '2018-03-26', '2018-05-31', '2017-03-31', '2018-03-30', '2018-04-02']

        self.assertEqual(results_with_base, expected_with_base)
        self.assertEqual(results_base_today, expected_base_today)

    def test_order_preference(self) -> None:
        text = '10111211, 04/03/2018, 11121011'

        extractions_dmy = de.extract(text, preferred_date_order='DMY', prefer_language_date_order=False)
        extractions_mdy = de.extract(text, preferred_date_order='MDY', prefer_language_date_order=False)
        extractions_ymd = de.extract(text, preferred_date_order='YMD', prefer_language_date_order=False)

        results_dmy = [e.value for e in extractions_dmy]
        results_mdy = [e.value for e in extractions_mdy]
        results_ymd = [e.value for e in extractions_ymd]

        expected_dmy = ['1211-11-10', '2018-03-04', '1011-12-11']
        expected_mdy = ['1211-10-11', '2018-04-03', '1011-11-12']
        expected_ymd = ['1011-12-11', '2018-04-03', '1112-10-11']

        self.assertEqual(results_dmy, expected_dmy)
        self.assertEqual(results_mdy, expected_mdy)
        self.assertEqual(results_ymd, expected_ymd)

    def test_timezone(self) -> None:
        text = '2018-12-25 12:30, 2018-12-25 11:00 -0830'

        extractions_1 = de.extract(text, to_timezone='MET', date_value_resolution=DateResolution.MINUTE)
        extractions_2 = de.extract(text, to_timezone='UTC', date_value_resolution=DateResolution.MINUTE)

        results_1 = [e.value for e in extractions_1]
        results_2 = [e.value for e in extractions_2]

        expected_1 = ['2018-12-25T09:30', '2018-12-25T20:30']
        expected_2 = ['2018-12-25T08:30', '2018-12-25T19:30']

        self.assertEqual(results_1, expected_1)
        self.assertEqual(results_2, expected_2)

    def test_prefer_day(self) -> None:
        text = '2018 July and 09/20 and 2017/12'

        results = [
            [e.value for e in de.extract(text, prefer_day_of_month='first')],
            [e.value for e in de.extract(text, prefer_day_of_month='last')]
        ]

        expected = [['2018-07-01', '2020-09-01', '2017-12-01'], ['2018-07-31', '2020-09-30', '2017-12-31']]

        self.assertEqual(results, expected)

    def test_language(self) -> None:
        text = 'el 29 de febrero de 1996 vs lunes, el 24 de junio, 2013 vs 3 de octubre de 2017, and 04/03/2010'

        results = [
            [e.value for e in de.extract(text, prefer_language_date_order=True, preferred_date_order='MDY')],
            [e.value for e in de.extract(text, prefer_language_date_order=False, preferred_date_order='MDY')]
        ]

        expected = [
            ['1996-02-29', '2013-06-24', '2017-10-03', '2010-03-04'],
            ['1996-02-29', '2013-06-24', '2017-10-03', '2010-04-03']
        ]

        self.assertEqual(results, expected)
