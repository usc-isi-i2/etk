from etk.timeseries.annotation import input_processor
import json
import numpy as np
import re
from etk.timeseries.annotation.table_processor import parsed_table
from etk.timeseries.annotation.granularity_detector import GranularityDetector


def is_number(value):
    if is_int(value) or is_float(value):
        return 1
    return 0


def is_empty(value):
    return (not value) or (value == ".")


def is_data(value):
    return is_number(value) or is_empty(value)


def is_int(value):
    if isinstance(value, int):
        return 1
    try:
        int(value)
        return 1
    except:
        return 0


def is_float(value):
    if isinstance(value, float):
        return 1
    try:
        float(value)
        return 1
    except:
        return 0


def is_string_literal(value):
    if isinstance(value, str):
        return True
    return False


def is_alpha(value):
    # Note the use of search instead of match
    # Match fails to match "1 kg"
    alpha_regex = re.compile(r'[a-z]', re.IGNORECASE)
    if is_string_literal(value) and alpha_regex.search(value):
        return 1
    return 0


def get_base_annotation(tp):
    annotation = """
        {
            "GlobalMetadata": [
                {
                    "name": "title",
                    "source": "sheet_name"
                }
            ],
            "Properties": {
                "sheet_indices": "[1]"
            },
            "TimeSeriesRegions": [
                {
                    "cols": "[%s]",
                    "locs": "[%s:%s]",
                    "metadata": [
                        %s
                    ],
                    "orientation": "col",
                    "times": {
                        "granularity": "%s",
                        "locs": "[%s]"
                    }
                }
            ]
        }
    """

    return json.loads(annotation % (tp['ts_column'], tp['start_row'], tp['end_row'],
                                    get_header_object(tp['header_present']),
                                    tp['granularity'],
                                    tp['time_column']))


def get_header_object(is_header_present=True):
    if is_header_present:
        return """
        {
            "loc": "[1]",
            "name": "label",
            "source": "row"
        }
        """
    else:
        return ""


def is_header_present(sheet):
    if len(sheet) == 0:
        return False

    if is_alpha(sheet[0][0]) and is_alpha(sheet[0][1]):
        return True


def remove_bom(sheet):
    try:
        if sheet[0][0].startswith("\uFEFF"):
            sheet[0][0] = sheet[0][0][1:]
    except:
        pass


class SimpleAnnotator:
    def __init__(self, infile):
        self.infile = infile

    def get_annotation_json(self):

        final_json = []

        for sheet, sheet_name, merged_cells in input_processor.process_excel(self.infile):
            table_properties = dict()

            sheet = sheet.to_array()
            sheet = np.array(sheet)

            remove_bom(sheet)

            row, col = sheet.shape

            # Two columns exactly
            assert col == 2

            # Check if header row is present
            header_present = is_header_present(sheet)
            start_row = 0
            if header_present:
                start_row = 1

            # Find the data block
            data_col = []
            for i in range(2):
                is_data_col = True
                for j in range(start_row, row):
                    if not is_data(sheet[j][i]):
                        is_data_col = False
                        break
                if is_data_col:
                    data_col.append(i)

            if len(data_col) == 2:
                logging.error("Ambiguous data. Multiple data columns present")
                break

            if len(data_col) == 0:
                logging.error("Cannot find data column")
                break

            data_col = data_col[0]
            # Other column is assumed to be date column
            date_col = data_col ^ 1

            table_properties['start_row'] = start_row + 1
            table_properties['end_row'] = row
            table_properties['time_column'] = parsed_table.get_excel_column_name(date_col)
            table_properties['header_present'] = header_present
            table_properties['ts_column'] = parsed_table.get_excel_column_name(data_col)
            table_properties['granularity'] = GranularityDetector.get_granularity(sheet[:, date_col])

            annotation = get_base_annotation(table_properties)

            final_json.append(annotation)

        return final_json
