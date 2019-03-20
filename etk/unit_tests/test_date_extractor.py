import unittest, datetime, pytz, json
from dateutil.relativedelta import relativedelta
from etk.extractors.date_extractor import DateExtractor, DateResolution
from etk.etk import ETK
from etk.knowledge_graph import KGSchema

kg_schema = KGSchema(json.load(open('etk/unit_tests/ground_truth/test_config.json')))
de = DateExtractor(ETK(kg_schema=kg_schema), 'unit_test_date')


class TestDateExtractor(unittest.TestCase):
    # auxiliary method
    @staticmethod
    def convert_to_iso_format(date: datetime.datetime, resolution: DateResolution = DateResolution.DAY) -> str or None:
        """

        Args:
            date: datetime.datetime - datetime object to convert
            resolution: resolution of the iso format date to return

        Returns: string of iso format date

        """
        # TODO: currently the resolution is specified by the user, should it be decided what we have extracted,
        # E.g.: like if only year exists, use DateResolution.YEAR as resolution
        try:
            if date:
                date_str = date.isoformat()
                length = len(date_str)
                if resolution == DateResolution.YEAR and length >= 4:
                    return date_str[:4]
                elif resolution == DateResolution.MONTH and length >= 7:
                    return date_str[:7]
                elif resolution == DateResolution.DAY and length >= 10:
                    return date_str[:10]
                elif resolution == DateResolution.HOUR and length >= 13:
                    return date_str[:13]
                elif resolution == DateResolution.MINUTE and length >= 16:
                    return date_str[:16]
                elif resolution == DateResolution.SECOND and length >= 19:
                    return date_str[:19]
                return date_str
        except Exception as e:
            return None

        return None

    def test_ground_truth(self) -> None:
        with open('etk/unit_tests/ground_truth/date_ground_truth.txt', 'r') as f:
            texts = f.readlines()
        for text in texts:
            text = text.strip()
            if text and text[0] != '#':
                temp = text.split('|')
                if len(temp) == 3:
                    input_text, expected, format = temp
                    ignore_before = datetime.datetime(1890, 1, 1)
                    ignore_after = datetime.datetime(2500, 10, 10)
                    relative_base = datetime.datetime(2018, 1, 1)

                    e = de.extract(input_text,
                                   extract_first_date_only=False,
                                   additional_formats=[format],
                                   use_default_formats=False,
                                   ignore_dates_before=ignore_before,
                                   ignore_dates_after=ignore_after,
                                   detect_relative_dates=not format,
                                   relative_base=relative_base,
                                   preferred_date_order="DMY",
                                   prefer_language_date_order=True,
                                   return_as_timezone_aware=False,
                                   prefer_day_of_month='first',
                                   prefer_dates_from='current',
                                   date_value_resolution=DateResolution.SECOND
                                   if format and len(format) > 1 and format[1] in ['H', 'I'] else DateResolution.DAY
                                   )
                    now_date = datetime.datetime.now()
                    expected = expected.replace('@today', self.convert_to_iso_format(now_date))
                    expected = expected.replace('@year', str(now_date.year))
                    if expected.startswith('@recentYear'):
                        today = datetime.datetime.now()
                        expected = expected.replace('@recentYear', str(today.year))
                        date = datetime.datetime.strptime(expected, '%Y-%m-%d')
                        next_year = date.replace(year=today.year + 1)
                        last_year = date.replace(year=today.year - 1)
                        if date > today and (date - today > today - last_year):
                            expected = str(last_year.year) + expected[4:]
                        elif date < today and (today - date > next_year - today):
                            expected = str(next_year.year) + expected[4:]
                    if expected and expected[0] != '@':
                        self.assertEqual(e[0].value if e else '', expected)

    def test_additional_formats(self) -> None:
        text = '2018@3@25  July 29 in 2018, 4/3@2018  2009-10-23 Jun 27 2017    D-3/8/91    S-07-08-2018'
        formats = ['%Y@%m@%d', '%B %d in %Y', '%m/%d@%Y', 'D-%d/%m/%y', 'S-%m-%d-%Y']

        extractions_with_default = de.extract(text, additional_formats=formats, use_default_formats=True)
        extractions_without_default = de.extract(text, additional_formats=formats, use_default_formats=False)

        results_with_default = [e.value for e in extractions_with_default]
        results_without_default = [e.value for e in extractions_without_default]

        expected_with_default = ['2018-03-25', '2018-07-29', '2018-04-03', '2009-10-23', '2017-06-27',
                                 '1991-08-03', '2018-07-08']
        expected_without_default = ['2018-03-25', '2018-07-29', '2018-04-03', '1991-08-03', '2018-07-08']

        self.assertEqual(results_with_default, expected_with_default)
        self.assertEqual(results_without_default, expected_without_default)

    def test_relative_date(self) -> None:
        text = '5 days ago, in two months, last year, yesterday, the day after tomorrow  ' \
               '2009-10-23 Jun 27 2017, what happened today'
        base = datetime.datetime(2018, 1, 1, tzinfo=pytz.timezone('UTC'))
        today = datetime.datetime.now()

        extractions_with_base = de.extract(text, detect_relative_dates=True, relative_base=base)
        extractions_base_tody = de.extract(text, detect_relative_dates=True)

        results_with_base = [e.value for e in extractions_with_base]
        results_base_today = [e.value for e in extractions_base_tody]

        relative = [relativedelta(days=-5), relativedelta(months=2), relativedelta(years=-1), relativedelta(days=-1),
                    relativedelta(days=2), relativedelta(days=0)]

        expected_with_base = ['2009-10-23', '2017-06-27'] + [self.convert_to_iso_format(base + x) for x in relative]
        expected_base_today = ['2009-10-23', '2017-06-27'] + [self.convert_to_iso_format(today + x) for x in relative]

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

    def test_prefer_day(self) -> None:
        text = '2018 July and 09/20 and 2017/12'

        results = [
            [e.value for e in de.extract(text, prefer_day_of_month='first', preferred_date_order='DMY')],
            [e.value for e in de.extract(text, prefer_day_of_month='last', preferred_date_order='DMY')]
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

    def test_original_resolution(self) -> None:
        text = '2019-10-23 | 2017-06 | 2018-03-10 10:12 | July 2018 | Mar 2000 | year 2020'

        extractions = de.extract(text, date_value_resolution=DateResolution.ORIGINAL)

        results = [e.value for e in extractions]

        expected = ['2019-10-23', '2017-06', '2018-03-10T10:12', '2018-07', '2000-03', '2020']

        self.assertEqual(results, expected)

    def test_corner_cases(self) -> None:
        text = 'That star is MARS. ' \
               'It happened in 2012.' \
               'he may go to his hometown' \
               '13X or the ratio is 69/44  ' \
               'I was born in 94/10 ' \
               'I will take vocation on 12/23'

        extractions = de.extract(text=text, date_value_resolution=DateResolution.ORIGINAL, prefer_dates_from='future')

        results = [e.value for e in extractions]
        today = datetime.datetime.now()
        year = today.year
        if datetime.datetime(datetime.datetime.now().year, 12, 23) < today:
            year += 1
        expected = ['2012', '1994-10', '%d-12-23' % year]

        self.assertEqual(results, expected)


if __name__ == '__main__':
    unittest.main()
