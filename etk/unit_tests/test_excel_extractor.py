import unittest
from etk.extractors.excel_extractor import ExcelExtractor

ee = ExcelExtractor()


class TestExcelExtractor(unittest.TestCase):
    def test_col_name_to_num(self) -> None:
        self.assertEquals(ee.col_name_to_num('A'), 0)
        self.assertEquals(ee.col_name_to_num('Z'), 25)
        self.assertEquals(ee.col_name_to_num('AA'), 26)
        self.assertEquals(ee.col_name_to_num('aA'), 26)
        self.assertEquals(ee.col_name_to_num('aa'), 26)

    def test_row_name_to_name(self) -> None:
        self.assertEquals(ee.row_name_to_num('1'), 0)
        self.assertEquals(ee.row_name_to_num('100'), 99)
        self.assertRaises(ValueError, ee.row_name_to_num, 'abc123')
        self.assertRaises(ValueError, ee.row_name_to_num, '0')

    def test_parse_variable(self) -> None:
        self.assertEquals(ee.parse_variable('$A', 10, 10), (0,))
        self.assertEquals(ee.parse_variable('$A+2,$1', 10, 10), (0, 2))
        self.assertEquals(ee.parse_variable('2+$A,$1', 10, 10), (0, 2))
        self.assertEquals(ee.parse_variable('$col,$1', 10, 10), (0, 10))
        self.assertEquals(ee.parse_variable('$A,$row', 10, 10), (10, 0))
        self.assertEquals(ee.parse_variable('$A+2,$row', 10, 10), (10, 2))


if __name__ == '__main__':
    unittest.main()
