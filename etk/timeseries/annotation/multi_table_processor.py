from etk.timeseries.annotation.table_processor import parsed_table
from etk.timeseries.annotation.utility import column_orientation
from etk.timeseries.annotation import date_utilities


class complex_table(parsed_table):
    # finding the overlapp in the time block based on monotonicity of the time block
    @classmethod
    def check_for_overlapping_time(cls, date_block, orientation, raw_sheet):
        check_points = []
        is_increasing = True
        if orientation == column_orientation:  # times are in columns
            check_points.append(date_block.upper_row)
            if date_block.lower_row - date_block.upper_row >= 2:
                d = date_utilities.parse_row_as_date(
                    raw_sheet[date_block.upper_row][date_block.left_col:date_block.right_col])
                d2 = date_utilities.parse_row_as_date(
                    raw_sheet[date_block.upper_row + 1][date_block.left_col:date_block.right_col])
                is_increasing = date_utilities.is_greater(d, d2)
            for row in range(date_block.upper_row, date_block.lower_row - 1):
                d = date_utilities.parse_row_as_date(raw_sheet[row][date_block.left_col:date_block.right_col])

                d2 = date_utilities.parse_row_as_date(raw_sheet[row + 1][date_block.left_col:date_block.right_col])
                if not date_utilities.is_greater(d, d2) and is_increasing:
                    check_points.append(row + 1)
                if not date_utilities.is_greater(d2, d) and not is_increasing:
                    check_points.append(row + 1)
            check_points.append(date_block.lower_row)
        else:
            check_points.append(date_block.left_col)
            check_points.append(date_block.right_col)
        # ToDO: write the same process for the row_orientation
        if len(check_points) == 2:
            return False, check_points

        return True, check_points
