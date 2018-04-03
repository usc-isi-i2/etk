import unittest
import json
from etk.extractors.csv_processor import CsvProcessor
from etk.etk import ETK

csv_str = """text,with,Polish,non-Latin,lettes
1,2,3,4,5,6
a,b,c,d,e,f

gęś,zółty,wąż,idzie,wąską,dróżką,
,b,c,s,w,f
"""

etk = ETK()


class TestCsvProcessor(unittest.TestCase):
    def test_csv_str_with_all_args(self) -> None:
        csv_processor = CsvProcessor(etk=etk,
                                     heading_row=1,
                                     content_start_row=2,
                                     heading_columns=(1, 3),
                                     content_end_row=2,
                                     ends_with_blank_row=True,
                                     remove_leading_empty_rows=True,
                                     required_columns=['text'])

        test_docs = [doc.cdr_document for doc in
                     csv_processor.tabular_extractor(table_str=csv_str, data_set='test_set')]
        expected_docs = [
            {'text': '1', 'with': '2', 'Polish': '3', 'non-Latin': '4', 'lettes': '5', 'data_set': 'test_set'},
            {'text': 'a', 'with': 'b', 'Polish': 'c', 'non-Latin': 'd', 'lettes': 'e', 'data_set': 'test_set'}]

        self.assertEqual(test_docs, expected_docs)

    def test_csv_str_with_ends_with_blank_row_false(self) -> None:
        csv_processor = CsvProcessor(etk=etk,
                                     heading_row=1,
                                     content_start_row=2,
                                     heading_columns=(1, 3),
                                     content_end_row=4,
                                     ends_with_blank_row=False,
                                     remove_leading_empty_rows=True,
                                     required_columns=['text'])

        test_docs = [doc.cdr_document for doc in
                     csv_processor.tabular_extractor(table_str=csv_str, data_set='test_set')]

        expected_docs = [
            {'text': '1', 'with': '2', 'Polish': '3', 'non-Latin': '4', 'lettes': '5', 'data_set': 'test_set'},
            {'text': 'a', 'with': 'b', 'Polish': 'c', 'non-Latin': 'd', 'lettes': 'e', 'data_set': 'test_set'},
            {'text': 'gęś', 'with': 'zółty', 'Polish': 'wąż', 'non-Latin': 'idzie', 'lettes': 'wąską',
             'data_set': 'test_set'}]

        self.assertEqual(test_docs, expected_docs)

    def test_csv_file_with_no_header(self) -> None:
        csv_processor = CsvProcessor(etk=etk,
                                     content_start_row=1,
                                     content_end_row=4,
                                     ends_with_blank_row=False,
                                     remove_leading_empty_rows=True)
        filename = 'etk/unit_tests/ground_truth/sample_csv.csv'

        test_docs = [doc.cdr_document for doc in
                     csv_processor.tabular_extractor(filename=filename, data_set='test_set')]

        expected_docs = [{'0': '', '1': 'name1', '2': 'name2', '3': '', '4': '', 'file_name': 'etk/unit_tests/ground_truth/sample_csv.csv', 'data_set': 'test_set'},
                         {'0': 'col11', '1': 'col12', '2': 'col13', '3': '', '4': 'col15', 'file_name': 'etk/unit_tests/ground_truth/sample_csv.csv', 'data_set': 'test_set'},
                         {'0': 'col21', '1': 'col22', '2': 'col23', '3': 'col24', '4': 'col25', 'file_name': 'etk/unit_tests/ground_truth/sample_csv.csv', 'data_set': 'test_set'}]

        self.assertEqual(test_docs, expected_docs)

    def test_csv_file_with_no_header_not_ends_with_blank_row(self) -> None:
        csv_processor = CsvProcessor(etk=etk,
                                     content_start_row=1,
                                     content_end_row=8,
                                     ends_with_blank_row=False,
                                     remove_leading_empty_rows=True)
        filename = 'etk/unit_tests/ground_truth/sample_csv.csv'

        test_docs = [doc.cdr_document for doc in
                     csv_processor.tabular_extractor(filename=filename, data_set='test_set')]

        expected_docs = [{'0': '', '1': 'name1', '2': 'name2', '3': '', '4': '', 'file_name': 'etk/unit_tests/ground_truth/sample_csv.csv', 'data_set': 'test_set'},
                         {'0': 'col11', '1': 'col12', '2': 'col13', '3': '', '4': 'col15', 'file_name': 'etk/unit_tests/ground_truth/sample_csv.csv', 'data_set': 'test_set'},
                         {'0': 'col21', '1': 'col22', '2': 'col23', '3': 'col24', '4': 'col25', 'file_name': 'etk/unit_tests/ground_truth/sample_csv.csv', 'data_set': 'test_set'},
                         {'0': 'col31', '1': 'col32', '2': 'col33', '3': 'col34', '4': 'col35', 'file_name': 'etk/unit_tests/ground_truth/sample_csv.csv', 'data_set': 'test_set'},
                         {'0': '', '1': '', '2': 'table1', '3': 'table2', '4': '', 'file_name': 'etk/unit_tests/ground_truth/sample_csv.csv', 'data_set': 'test_set'}]

        self.assertEqual(test_docs, expected_docs)
