import unittest
from etk.extractors.table_extractor import TableExtractor, EntityTableDataExtraction


class TestTableExtractor(unittest.TestCase):
    test_doc = '''
        <html>
        <body>
        test random text!
        <table><tbody>
        <tr> 
        <th>caliber:</th><td>0.45 mm</td>
        </tr>
        <tr> 
        <th>manufacturer:</th><td>WXC</td>
        </tr>
        <tr> 
        <th>country:</th><td>Russia</td>
        </tr>
        <tr> 
        <th>price:</th><td>400 eur</td>
        </tr>
        </tbody></table>
        <body>
        </html>
    '''

    def test_table_extractor(self) -> None:
        my_table_extractor = TableExtractor()
        res = my_table_extractor.extract(TestTableExtractor.test_doc)
        self.assertEqual(len(res), 1)
        res = my_table_extractor.extract(TestTableExtractor.test_doc, True)
        self.assertEqual(len(res), 2)
        self.assertEqual(res[0].value, "test random text!")

    def test_entity_table_data_extractor(self) -> None:
        expected_res = [("caliber", "0.45 mm"),
                        ("manufacturer", "WXC"),
                        ("location", "Russia"),
                        ("price", "400 eur")]
        my_table_extractor = TableExtractor()
        table_data_extractor = EntityTableDataExtraction()
        table_data_extractor.add_glossary(["caliber", "calibre"], "caliber")
        table_data_extractor.add_glossary(["manufacturer"], "manufacturer")
        table_data_extractor.add_glossary(["price"], "price")
        table_data_extractor.add_glossary(["country"], "location")
        res = my_table_extractor.extract(TestTableExtractor.test_doc)
        res = table_data_extractor.extract(res[0].value)
        res = [(x.tag, x.value) for x in res]
        self.assertEqual(res, expected_res)


if __name__ == '__main__':
    unittest.main()