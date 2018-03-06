import unittest, json
from etk.extractors.date_extractor import DateExtractor


class TestDateExtractor(unittest.TestCase):

    def test_date_extractor(self) -> None:
        date_extractor = DateExtractor('test_date_parser')
        text = '03/05/2018: I went to USC on Aug 20th, 2016 and will graduate on 2018, May 11. My birthday is 29-04-1994.'

        test_result = [x.value for x in date_extractor.extract(text, False, 30)]

        expected = ['2018-03-05T00:00:00',
                    '2016-08-20T00:00:00',
                    '2018-05-11T00:00:00',
                    '1994-04-29T00:00:00'
                    ]
        self.assertEqual(json.dumps(test_result), json.dumps(expected))


if __name__ == '__main__':
    unittest.main()