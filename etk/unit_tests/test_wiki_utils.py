import unittest
from etk.wikidata.utils import *


class TestWikidataUtils(unittest.TestCase):
    def test_parse_datetime_string(self):
        self.assertEqual(
            parse_datetime_string('19710101 01:02:03', additional_formats=['%Y%m%d %H:%M:%S']),
            ('1971-01-01T01:02:03', Precision.second))
        self.assertEqual(
            parse_datetime_string('19710101 01:02', additional_formats=['%Y%m%d %H:%M']),
            ('1971-01-01T01:02:00', Precision.minute))
        self.assertEqual(
            parse_datetime_string('19710101 1am', additional_formats=['%Y%m%d %Ham']),
            ('1971-01-01T01:00:00', Precision.hour))
        self.assertEqual(
            parse_datetime_string('19710101'),
            ('1971-01-01T00:00:00', Precision.day))
        self.assertEqual(
            parse_datetime_string('197101'),
            ('1971-01-01T00:00:00', Precision.month))
        self.assertEqual(
            parse_datetime_string('1971', additional_formats=['%Y']),
            ('1971-01-01T00:00:00', Precision.year))
