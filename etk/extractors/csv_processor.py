import pyexcel_io
import pyexcel_xlsx
import os
import csv
from etk.document import Document
from typing import List
from io import StringIO


class CsvProcessor(object):
    """
                        heading_row: int,
                        content_start_row: int,
                        heading_columns: (int, int),
                        content_end_row: int,
                        blank_row_ends_content: bool,
                        remove_leading_trailing_whitespace: bool,
                        required_columns: list[str]
    """

    def __init__(self, etk, **mapping_spec) -> None:
        self.etk = etk

        self.heading_row = mapping_spec.get("heading_row")
        if self.heading_row is not None:
            self.heading_row = self.heading_row - 1

        self.content_start_row = mapping_spec.get("content_start_row", 2) - 1

        # how about if heading_col is not present? read until first empty cell?
        self.heading_columns = mapping_spec.get("heading_columns")

        if self.heading_columns is not None:
            self.heading_columns = (self.heading_columns[0] - 1, self.heading_columns[1] + 1)

        # if not present, default read until an empty row
        self.content_end_row = mapping_spec.get("content_end_row")
        # if self.content_end_row is not None:
        #     self.content_end_row = self.content_end_row

        # if set to false, read until EOF
        self.blank_row_ends_content = mapping_spec.get("ends_with_blank_row", True)

        # remove all white space of the value
        self.remove_leading_trailing_whitespace = \
            mapping_spec.get("remove_leading_trailing_whitespace", True)

        # remove all empty rows before the content
        self.remove_leading_empty_rows = \
            mapping_spec.get("remove_leading_empty_rows", True)

        self.required_columns = mapping_spec.get("required_columns")

        self._get_data_function = {
            ".csv": pyexcel_io.get_data,
            ".tsv": pyexcel_io.get_data,
            ".xls": pyexcel_xlsx.get_data,
            ".xlsx": pyexcel_xlsx.get_data
        }

    def tabular_extractor(self, table_str: str = None, filename: str = None,
                          sheet_name:str = None,
                          data_set: str = None,
                          nested_key: str = None) -> List[Document]:
        data = list()

        if table_str is not None and filename is not None:
            print("please only specify one argument!")
            return list()
        elif table_str is not None:
            f = StringIO(table_str)
            reader = csv.reader(f, delimiter=',')
            for row in reader:
                data.append(row)
        elif filename is not None:
            # always read the entire file first
            fn, extension = os.path.splitext(filename)

            if extension in self._get_data_function:
                get_data = self._get_data_function[extension]
            else:
                print("file extension can not read")
                return list()

            try:
                data = get_data(filename, auto_detect_datetime=False,
                                auto_detect_float=False, encoding="utf-8")
            except:
                try:
                    data = get_data(filename, auto_detect_datetime=False,
                                    auto_detect_float=False, encoding="latin_1")
                except:
                    data = get_data(filename, auto_detect_datetime=False,
                                    auto_detect_float=False, encoding="utf-8-sig")

            if extension == '.xls' or extension == '.xlsx':
                if sheet_name is None:
                    sheet_name = data.key()[0]
                    print("ssss")
                    print(sheet_name)

                data = data[sheet_name]
            else:
                file_name = fn.split('/')[-1] + extension
                data = data[file_name]

        table_content, header = self.content_recognizer(data)

        return self.create_documents(table_content, header, filename, data_set, nested_key)

    def content_recognizer(self, data: List[List[str]]) -> tuple((List[List[str]], List[str])):
        heading = list()
        if self.heading_row is not None:
            heading, col_start, col_end = self.process_header(data[self.heading_row])
            # if heading_row is specified, discards/overwrite the heading_columns
            self.heading_columns = (col_start, col_end)
        else:
            heading = None

        # handle row first:
        if self.content_end_row is not None:
            data = data[self.content_start_row:self.content_end_row]

        data, col_start, col_end, row_count = self.process_by_row(data)
        if self.heading_columns is None:
            self.heading_columns = (col_start, col_end)

        # handle col:
        data = self.extract_row_content(data)

        return data, heading

    @staticmethod
    def process_header(header: List[str]) -> tuple((List[str], int, int)):
        processed_header = list()
        col_start, col_end = 0, len(header)
        for i in range(len(header)):
            if not header[i] and col_start == 0:
                continue
            elif header[i] and col_start == 0:
                col_start = i
                processed_header.append(header[i])
            elif not header[i] and col_start != 0:
                col_end = i
                break
            else:
                processed_header.append(header[i])

        return processed_header, col_start-1, col_end

    # slicing table by start and end col
    def extract_row_content(self, sheet: List[List[str]]) -> List[List[str]]:
        valid_row = list()
        for row in sheet:
            valid_row.append(row[self.heading_columns[0]:self.heading_columns[1]])

        return valid_row

    # TODO: refactor this method
    # remove all leading empty rows and figure out the start and end col
    def process_by_row(self, sheet: List[List[str]]) -> tuple((List[List[str]], int, int, int)):
        col_min = float("inf")
        col_max = 0
        row_count = 0
        valid_row = list()

        for row in sheet:
            # first row & row is empty & remove all leading empty rows
            if row_count == 0 and not any(row) and self.remove_leading_empty_rows:
                continue
            elif row_count == 0 and not any(row) and not self.remove_leading_empty_rows:
                row_count += 1
                continue
            # not first row & row is empty & end with blank row
            elif row_count != 0 and not any(row) and self.blank_row_ends_content:
                break
            elif row_count != 0 and not any(row) and not self.blank_row_ends_content:
                row_count += 1
                continue

            #  find first non-empty col
            idx = list(map(bool, row)).index(True)
            col_min = min(col_min, idx)
            col_max = max(len(row), col_max)
            row_count += 1
            valid_row.append(row)

        return valid_row, col_min, col_max, row_count

    def create_documents(self, rows: List[List[str]],
                         header: List[str] = None,
                         file_name: str = None,
                         data_set: str = None,
                         nested_key: str = None) -> List[Document]:
        documents = list()
        # etk = ETK()
        if self.heading_row is None and self.required_columns is not None:
            print("cannot match the required columns since heading is not specified")
            return list()

        # get the header line index of required columns
        list_idx = list()
        if self.required_columns is not None:
            for i in range(len(header)):
                if header[i] in self.required_columns:
                    list_idx.append(i)
        # filter each row
        for row in rows:
            # if the row is empty, skip it
            if not any(row):
                continue

            is_required_not_empty = all(row[i] for i in list_idx)
            if not is_required_not_empty:
                continue

            doc = dict()
            for i in range(0, self.heading_columns[1] - self.heading_columns[0]):
                # range(len(row)):
                if header is not None:
                    key = header[i]
                else:
                    key = str(i)

                if i >= len(row):
                    doc[key] = ''
                else:
                    doc[key] = row[i]

            cdr_doc = dict()
            if nested_key is not None:
                cdr_doc[nested_key] = doc
            else:
                cdr_doc = doc

            if file_name is not None:
                cdr_doc['file_name'] = file_name
            if data_set is not None:
                cdr_doc['data_set'] = data_set

            documents.append(self.etk.create_document(cdr_doc))

        return documents
