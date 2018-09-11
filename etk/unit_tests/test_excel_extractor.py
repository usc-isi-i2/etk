import unittest
from etk.extractors.excel_extractor import ExcelExtractor
import datetime

ee = ExcelExtractor()
file_path = 'etk/unit_tests/ground_truth/NST-Main Sheet.xlsx'
sheet_name = 'NST Main Dataset'


class TestExcelExtractor(unittest.TestCase):
    def test_cur_col_cur_row(self) -> None:
        extractions = ee.extract(file_name=file_path,
                                sheet_name=sheet_name,
                                region=['A,2', 'B,10'],
                                variables={
                                    'value': '$col,$row',
                                    })
        expected = [{'value': "Army Barracks Attack Ahead of Goodluck Jonathan's Inauguration"},
                    {'value': 'Kaduna'},
                    {'value': 'Morning Attack in Zaria'},
                    {'value': "Shehu of Borno's Brother Killed"},
                    {'value': 'Attempted Attack on Government Vaccine Warehouse'},
                    {'value': 'Militants Kill Muslim Cleric Ibrahim Burkuti'},
                    {'value': "Bomb Blasts Outside St. Patrick's Catholic Church"},
                    {'value': 'Bombing of Kano Motor Park'}
                    ]

        self.assertEqual(extractions, expected)

    def test_const_col(self) -> None:
        extractions = ee.extract(file_name=file_path,
                               sheet_name=sheet_name,
                               region=['A,2', 'B,10'],
                               variables={
                                   'value': '$B'})
        expected = [{'value': 1},
                    {'value': 1},
                    {'value': 1},
                    {'value': 1},
                    {'value': 1},
                    {'value': 1},
                    {'value': 1},
                    {'value': 1}
                    ]

        self.assertEqual(extractions, expected)

    def test_const_col_const_row(self) -> None:
        extractions = ee.extract(file_name=file_path,
                                sheet_name=sheet_name,
                                region=['A,2', 'B,10'],
                                variables={
                                    'value': '$A,$10',
                                    })
        expected = [{'value': 'David Usman and  Shot Dead'},
                    {'value': 'David Usman and  Shot Dead'},
                    {'value': 'David Usman and  Shot Dead'},
                    {'value': 'David Usman and  Shot Dead'},
                    {'value': 'David Usman and  Shot Dead'},
                    {'value': 'David Usman and  Shot Dead'},
                    {'value': 'David Usman and  Shot Dead'},
                    {'value': 'David Usman and  Shot Dead'}
                    ]

        self.assertEqual(extractions, expected)

    def test_offset_col_const_row(self) -> None:
        extractions = ee.extract(file_name=file_path,
                                sheet_name=sheet_name,
                                region=['A,2', 'B,10'],
                                variables={
                                    'value': '$A+1,$row',
                                    })
        expected = [{'value': datetime.date(2011, 5, 29)},
                    {'value': datetime.date(2011, 5, 29)},
                    {'value': datetime.date(2011, 5, 30)},
                    {'value': datetime.date(2011, 5, 30)},
                    {'value': datetime.date(2011, 6, 2)},
                    {'value': datetime.date(2011, 6, 6)},
                    {'value': datetime.date(2011, 6, 7)},
                    {'value': datetime.date(2011, 6, 7)}
                    ]

        self.assertEqual(extractions, expected)

    def test_cur_col_const_row(self) -> None:
        extractions = ee.extract(file_name=file_path,
                                 sheet_name=sheet_name,
                                 region=['A,2', 'C,10'],
                                 variables={
                                     'value': '$col,$10',
                                 })

        expected = [{'value': 'David Usman and  Shot Dead'},
                    {'value': datetime.date(2011, 6, 7)},
                    {'value': 'David Usman and  Shot Dead'},
                    {'value': datetime.date(2011, 6, 7)},
                    {'value': 'David Usman and  Shot Dead'},
                    {'value': datetime.date(2011, 6, 7)},
                    {'value': 'David Usman and  Shot Dead'},
                    {'value': datetime.date(2011, 6, 7)},
                    {'value': 'David Usman and  Shot Dead'},
                    {'value': datetime.date(2011, 6, 7)},
                    {'value': 'David Usman and  Shot Dead'},
                    {'value': datetime.date(2011, 6, 7)},
                    {'value': 'David Usman and  Shot Dead'},
                    {'value': datetime.date(2011, 6, 7)},
                    {'value': 'David Usman and  Shot Dead'},
                    {'value': datetime.date(2011, 6, 7)}]

        self.assertEqual(extractions, expected)

    def test_const_col_cur_row(self) -> None:
        extractions = ee.extract(file_name=file_path,
                                 sheet_name=sheet_name,
                                 region=['A,2', 'B,10'],
                                 variables={
                                     'value': '$A,$row',
                                 })
        expected = [{'value': "Army Barracks Attack Ahead of Goodluck Jonathan's Inauguration"},
                    {'value': 'Kaduna'},
                    {'value': 'Morning Attack in Zaria'},
                    {'value': "Shehu of Borno's Brother Killed"},
                    {'value': 'Attempted Attack on Government Vaccine Warehouse'},
                    {'value': 'Militants Kill Muslim Cleric Ibrahim Burkuti'},
                    {'value': "Bomb Blasts Outside St. Patrick's Catholic Church"},
                    {'value': 'Bombing of Kano Motor Park'}]

        self.assertEqual(extractions, expected)

    # def test_col_name_to_num(self) -> None:
    #     self.assertEquals(ee.col_name_to_num('A'), 0)
    #     self.assertEquals(ee.col_name_to_num('Z'), 25)
    #     self.assertEquals(ee.col_name_to_num('AA'), 26)
    #     self.assertEquals(ee.col_name_to_num('aA'), 26)
    #     self.assertEquals(ee.col_name_to_num('aa'), 26)
    #
    # def test_row_name_to_name(self) -> None:
    #     self.assertEquals(ee.row_name_to_num('1'), 0)
    #     self.assertEquals(ee.row_name_to_num('100'), 99)
    #     self.assertRaises(ValueError, ee.row_name_to_num, 'abc123')
    #     self.assertRaises(ValueError, ee.row_name_to_num, '0')
    #
    # def test_parse_variable(self) -> None:
    #     self.assertEquals(ee.parse_variable('$A', 10, 10), (0,))
    #     self.assertEquals(ee.parse_variable('$A+2,$1', 10, 10), (0, 2))
    #     self.assertEquals(ee.parse_variable('2+$A,$1', 10, 10), (0, 2))
    #     self.assertEquals(ee.parse_variable('$col,$1', 10, 10), (0, 10))
    #     self.assertEquals(ee.parse_variable('$A,$row', 10, 10), (10, 0))
    #     self.assertEquals(ee.parse_variable('$A+2,$row', 10, 10), (10, 2))


if __name__ == '__main__':
    unittest.main()
