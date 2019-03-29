import pyexcel_io
import pyexcel_xlsx
import os
import csv
import datetime
from etk.document import Document
from typing import List
from io import StringIO
from etk.etk_exceptions import InvalidArgumentsError, InvalidFilePathError
import pandas as pd


class CsvProcessor(object):
    """
                        heading_row: int,
                        content_start_row: int,
                        heading_columns: (int, int),
                        content_end_row: int,
                        blank_row_ends_content: bool,
                        remove_leading_trailing_whitespace: bool,
                        required_columns: list[str]
                        column_name_prefix: str
    """

    def __init__(self, etk, **mapping_spec) -> None:
        self.etk = etk

        self.heading_row = mapping_spec.get("heading_row")
        if self.heading_row is not None:
            self.heading_row = self.heading_row - 1

        if self.heading_row is not None:
            self.content_start_row = mapping_spec.get(
                "content_start_row", self.heading_row + 2) - 1

        if self.heading_row is None:
            self.content_start_row = mapping_spec.get(
                "content_start_row", 1) - 1

        # how about if heading_col is not present? read until first empty cell?
        self.heading_columns = mapping_spec.get("heading_columns")

        if self.heading_columns is not None:
            self.heading_columns = (self.heading_columns[
                                    0] - 1, self.heading_columns[1])

        # if not present, default read until an empty row
        self.content_end_row = mapping_spec.get("content_end_row")

        # if set to false, read until EOF
        self.blank_row_ends_content = mapping_spec.get(
            "ends_with_blank_row", True)

        # remove all white space of the value
        self.remove_leading_trailing_whitespace = \
            mapping_spec.get("remove_leading_trailing_whitespace", True)

        # remove all empty rows before the content
        self.remove_leading_empty_rows = \
            mapping_spec.get("remove_leading_empty_rows", True)

        self.required_columns = mapping_spec.get("required_columns")

        if mapping_spec.get("column_name_prefix"):
            self.column_name_prefix = mapping_spec.get("column_name_prefix")
        else:
            self.column_name_prefix = "C"

        self._get_data_function = {
            ".csv": pyexcel_io.get_data,
            ".tsv": pyexcel_io.get_data,
            ".xls": pyexcel_xlsx.get_data,
            ".xlsx": pyexcel_xlsx.get_data
        }

    def tabular_extractor(self, table_str: str = None, filename: str = None,
                          file_content=None,
                          file_type=None,
                          sheet_name: str = None,
                          dataset: str = None,
                          nested_key: str = None,
                          doc_id_field: str = None,
                          dataframe: pd.DataFrame = None,
                          encoding=None,
                          fillnan=None,
                          df_string=False) -> List[Document]:
        """
        Read the input file/content and return a list of Document(s)
        Args:
            table_str: use this parameter, if you are 100% sure that the content is a csv
            filename: use this parameter if the file extension is one of tab, csv, tsv, xls, xlsx
            file_content: if the input has some arbitrary extension, read it yourself and pass the contents along
            file_type: use this parameter with file_content, can be tsv, csv, etc
            sheet_name: sheet name as in xls or xlsx files
            dataset: user provided string to be added to output Document(s)
            nested_key: user provided string to be added to output Document(s)
            doc_id_field: specify this field(should be present in the input file), its value will be used as doc_id
            dataframe: use this parameter if the contents being passed along are a pandas DataFrame
            fillnan: specify the value to be filled for NaNs in the dataframe
            df_string: converts all dataframe columns to type str

        Returns: List[Document]

        """
        data = list()

        if (table_str is not None and filename is not None) or (dataframe is not None and filename is not None) or (table_str is not None and dataframe is not None):
            raise InvalidArgumentsError(message="for arguments 'table_str', 'filename' and 'dataframe', please specify only one "
                                                "argument!")

        elif table_str is not None:
            f = StringIO(table_str)
            reader = csv.reader(f, delimiter=',')
            for row in reader:
                data.append(row)

        elif dataframe is not None:
            if self.heading_row is not None and self.heading_row > 1:
                raise InvalidArgumentsError(
                    message="Use pandas skiprows to decide the heading row!")
            if fillnan is not None:
                dataframe = dataframe.fillna(fillnan)
            if df_string:
                dataframe = dataframe.astype(str)
            data = [dataframe.columns.values.tolist()] + \
                dataframe.values.tolist()
        elif filename is not None:
            # always read the entire file first
            fn, extension = os.path.splitext(filename)
            extension = extension.lower()

            if extension in self._get_data_function:
                get_data = self._get_data_function[extension]
            else:
                # in pyexcel we trust
                # if there is an extension we have not mapped, just let pyexcel
                # figure it out
                get_data = pyexcel_io.get_data

            try:
                if file_content and file_type:
                    data = get_data(file_content, file_type=file_type, auto_detect_datetime=False,
                                    auto_detect_float=False, encoding=encoding if encoding else "utf-8")
                else:
                    data = get_data(filename, auto_detect_datetime=False,
                                    auto_detect_float=False, encoding=encoding if encoding else "utf-8")
            except:
                try:
                    data = get_data(filename, auto_detect_datetime=False,
                                    auto_detect_float=False, encoding="latin_1")
                except:
                    data = get_data(filename, auto_detect_datetime=False,
                                    auto_detect_float=False, encoding="utf-8-sig")

            if extension == '.xls' or extension == '.xlsx':
                if sheet_name is None:
                    sheet_name = list(data.keys())[0]

                data = data[sheet_name]
            else:
                data = data[file_type] if file_type else data[
                    fn.split('/')[-1] + extension]

        table_content, heading = self.content_recognizer(data)
        return self.create_documents(rows=table_content,
                                     heading=heading,
                                     file_name=filename,
                                     dataset=dataset,
                                     nested_key=nested_key,
                                     doc_id_field=doc_id_field)

    def content_recognizer(self, data: List[List[str]]) -> tuple((List[List[str]], List[str])):
        heading = list()
        # process heading
        if self.heading_row is not None:
            heading, col_start, col_end = self.process_heading(
                data[self.heading_row])
            # if heading_row is specified, discards/overwrite the
            # heading_columns
            self.heading_columns = (col_start, col_end)
        else:
            heading = None

        # handle row first:
        if self.content_end_row is None:
            data = data[self.content_start_row:]

        if self.content_end_row is not None:
            data = data[self.content_start_row:self.content_end_row]

        data, col_start, col_end, row_count = self.process_by_row(data)
        if self.heading_columns is None:
            self.heading_columns = (col_start, col_end)

        # handle col:
        data = self.extract_row_content(data)

        return data, heading

    @staticmethod
    def process_heading(heading: List[str]) -> tuple((List[str], int, int)):
        processed_heading = list()
        col_start, col_end = 0, len(heading)
        for i in range(len(heading)):
            if not heading[i] and col_start == 0:
                continue
            elif heading[i] and col_start == 0:
                col_start = i
                processed_heading.append(heading[i])
            elif not heading[i] and col_start != 0:
                col_end = i
                break
            else:
                processed_heading.append(heading[i])

        return processed_heading, col_start - 1, col_end

    # slicing table by start and end col
    def extract_row_content(self, sheet: List[List[str]]) -> List[List[str]]:
        valid_row = list()
        for row in sheet:
            valid_row.append(
                row[self.heading_columns[0]:self.heading_columns[1]])

        return valid_row

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
                         heading: List[str] = None,
                         file_name: str = None,
                         dataset: str = None,
                         nested_key: str = None,
                         doc_id_field: str = None) -> List[Document]:
        documents = list()
        # etk = ETK()
        if self.heading_row is None and self.required_columns is not None:
            raise InvalidArgumentsError(
                "cannot match the required columns since heading is not specified")

        # get the heading line index of required columns
        list_idx = list()
        if self.required_columns is not None:
            for i in range(len(heading)):
                if heading[i] in self.required_columns:
                    list_idx.append(i)
        # filter each row
        for row in rows:
            # if the row is empty, skip it
            if not any(row):
                continue
            # if some required field is missing, skip it
            is_required_not_empty = all(row[i] for i in list_idx)
            if not is_required_not_empty:
                continue
            # convert datetime obj to ISO format str
            row = self.datetime_to_string(row)
            # create doc for each row
            doc = dict()
            for i in range(0, self.heading_columns[1] - self.heading_columns[0]):
                if heading is not None:
                    key = heading[i]
                else:
                    key = self.column_name_prefix + str(i)

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
            if dataset is not None:
                cdr_doc['dataset'] = dataset

            doc_id = None
            if doc_id_field:
                doc_id = cdr_doc.get(doc_id_field, None)
            documents.append(self.etk.create_document(cdr_doc, doc_id=doc_id))

        return documents

    # convert datetime object of list to ISO format string
    @staticmethod
    def datetime_to_string(row: List[object]) -> List[object]:
        for i in range(len(row)):
            if type(row[i]) is datetime.date:
                row[i] = row[i].isoformat()

        return row
