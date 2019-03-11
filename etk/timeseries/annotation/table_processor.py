from etk.timeseries.annotation import block_detector
from etk.timeseries.annotation import date_utilities
from etk.timeseries.annotation import utility
import logging


class parsed_table:
    # we defind a table when we find a time header for that
    def __init__(self, time_header, parent_sheet):
        self.time_block = time_header
        self.parent_sheet = parent_sheet
        self.time_orientation = self.find_orientation()
        self.find_series_block()  # location of time series data
        self.labels = []
        self.label_names = {}
        self.label_backfill_bool = {}
        self.borders = None
        self.granularity = None
        self.find_granularity()
        self.back_mode_time = False
        self.break_points = []
        self.time_block_length = self.find_time_block_length()
        self.offset = 0

    # check if there exists consecutive rows in the time block which are parsed to a unique time
    def has_equivalent_time_rows(self):
        for i in range(self.time_block.upper_row, self.time_block.lower_row-1):
            p_date1 = date_utilities.parse_row_as_date(self.parent_sheet.raw_values[i][self.time_block.left_col:self.time_block.right_col])
            p_date2 = date_utilities.parse_row_as_date(self.parent_sheet.raw_values[i+1][self.time_block.left_col:self.time_block.right_col])
            if p_date1.equals(p_date2):
                return True
        return False

    def has_equivalent_time_columns(self):
        for i in range(self.time_block.left_col, self.time_block.right_col-1):
            tb = (list(self.parent_sheet.raw_values.columns()))
            p_date1 = date_utilities.parse_row_as_date(list(self.parent_sheet.raw_values.columns())[i][self.time_block.upper_row:self.time_block.lower_row])
            p_date2 = date_utilities.parse_row_as_date(list(self.parent_sheet.raw_values.columns())[i+1][self.time_block.upper_row:self.time_block.lower_row])
            if p_date1.equals(p_date2):
                return True
        return False

    ##important: using this offset list for granularity would be a good idea. specially when they are irregular
    # also it can be used for them to create annotation for files with irregular merged cells
    def has_regular_offset(self):
        offsets = []
        if self.time_orientation == utility.row_orientation:
            current_pattern = 0
            for i in range(self.time_block.left_col, self.time_block.right_col - 1):
                if date_utilities.parse_row_as_date(list(self.parent_sheet.raw_values.columns())[i][self.time_block.upper_row:self.time_block.lower_row]).equals(
                        date_utilities.parse_row_as_date(list(self.parent_sheet.raw_values.columns())[i + 1][self.time_block.upper_row:self.time_block.lower_row])):
                    current_pattern += 1
                else:
                    offsets.append(current_pattern)
                    current_pattern = 0

        else:
            current_pattern = 0
            for i in range(self.time_block.upper_row, self.time_block.lower_row - 1):
                if date_utilities.parse_row_as_date(self.parent_sheet.raw_values[i][self.time_block.left_col:self.time_block.right_col]).equals(date_utilities.parse_row_as_date(self.parent_sheet.raw_values[i + 1][self.time_block.left_col:self.time_block.right_col])):
                    current_pattern += 1
                else:
                    offsets.append(current_pattern)
                    current_pattern = 0
        # check for regularity of the given offset
        for i in range(len(offsets)-1):
            if offsets[i] != offsets[i+1]:
                return False, 0
        # Case when offset list is empty. Happens when all the dates are same in a range. Eg. [2007, 2007, 2007]
        if len(offsets) == 0:
            return False, 0

        return True, offsets[0]

    # check for the effect of merged time cells. non zero offset means multiple tables. Now only regular case is supported
    def check_offset(self):
        if self.time_orientation == utility.row_orientation:

            if self.has_equivalent_time_columns():
                regular, offset = self.has_regular_offset()
                if regular:
                    self.offset = offset
        else:
            if self.has_equivalent_time_rows():
                regular, offset = self.has_regular_offset()
                if regular:
                    self.offset = offset

    def find_granularity(self):
        if self.time_orientation == utility.column_orientation:
            self.granularity = date_utilities.find_granularity(list(self.parent_sheet.raw_values.rows())[self.time_block.upper_row:self.time_block.lower_row],
                                                          self.time_block.left_col, self.time_block.right_col)
        else:
            self.granularity = date_utilities.find_granularity(
                list(self.parent_sheet.raw_values.columns())[self.time_block.left_col:self.time_block.right_col],
                self.time_block.upper_row, self.time_block.lower_row)
            logging.info("Granularity of %s is %s", str(self.time_block), self.granularity)

    # find the granularity of the specified interval of the time block
    def find_bounded_granularity(self, start, end):
        if self.time_orientation == utility.column_orientation:
            self.granularity = date_utilities.find_granularity(list(self.parent_sheet.raw_values.rows())[start:end],
                                                          self.time_block.left_col, self.time_block.right_col)
        else:
            self.granularity = date_utilities.find_granularity(list(self.parent_sheet.raw_values.columns())[start:end],self.time_block.upper_row, self.time_block.lower_row)

        logging.info("Granularity of %s is %s", str(self.time_block), self.granularity)
        return self.granularity

    def get_orientation(self):
        return self.time_orientation

    def get_granularity(self):
        return self.granularity

    def add_label(self, label_block):
        self.labels.append(label_block)
        self.update_data_block(label_block)

    # detecting the number/empty blocks which are data of the time serieses
    def find_series_block(self):
        start, end = block_detector.find_data_block(self.parent_sheet.raw_values, self.parent_sheet.get_tags(), self.time_block.upper_row, self.time_block.lower_row, self.time_block.left_col,
                                                              self.time_block.right_col, self.time_orientation)
        self.data_start = start
        self.data_end = end

    # if there are some labels found and data was not found for them then should update the location of time series data
    def update_data_block(self, label_block):
        if self.time_orientation == utility.row_orientation:
            if label_block.lower_row > self.data_end:
                self.data_end = label_block.lower_row
        else:
            if label_block.right_col > self.data_end:
                self.data_end = label_block.right_col

    # fill the empty cells in the time block.[the back_fill mode case] (should be based on orientation)
    def fill_time_block(self):
        for i in range(self.time_block.upper_row, self.time_block.lower_row):
            for j in range(self.time_block.left_col, self.time_block.right_col):
                if self.parent_sheet.get_tags()[i][j] == {utility.empty_cell}:
                    # go and find the closest full cell and get the same value for that
                    row, col = block_detector.find_closest_date_cell(self.parent_sheet.get_tags(), i, j, self.time_block)
                    self.parent_sheet.raw_values[i, j] =  self.parent_sheet.raw_values[row, col]
                    self.parent_sheet.classified_tags[i][j] = self.parent_sheet.classified_tags[row][col]
                    self.back_mode_time = True

    def find_orientation(self):
        self.fill_time_block()
        if self.time_block.right_col - self.time_block.left_col == 1:
            return utility.column_orientation

        if self.time_block.lower_row - self.time_block.upper_row == 1:
            return utility.row_orientation
        else:
            logging.info("finding orientation in the worst case")
            # if row is a meaningful absolute time then it can be in columns
            if date_utilities.valid_time_row(self.parent_sheet.raw_values[self.time_block.upper_row][self.time_block.left_col:self.time_block.right_col])[0]:
                logging.error("time row was not consistent!")
                return utility.column_orientation

        return utility.row_orientation

    def find_table_borders(self, tags):
        table_borders = block_detector.rectangular_block()
        table_borders.left_col = self.time_block.left_col
        table_borders.right_col = self.time_block.right_col
        table_borders.upper_row = self.time_block.upper_row
        table_borders.lower_row = self.time_block.lower_row

        for l_block in self.labels:
            table_borders.left_col = min(table_borders.left_col , l_block.left_col)
            table_borders.right_col = max(table_borders.right_col, l_block.right_col)
            table_borders.upper_row = min(table_borders.upper_row, l_block.upper_row)
            table_borders.lower_row = max(table_borders.lower_row, l_block.lower_row)
        if self.time_orientation == utility.column_orientation:
            table_borders.left_col = min(table_borders.left_col, self.data_start)
            table_borders.right_col = max(table_borders.right_col, self.data_end)
        else:
            table_borders.upper_row = min(table_borders.upper_row, self.data_start)
            table_borders.lower_row = max(table_borders.lower_row, self.data_end)
        # continue from side untill reaching a place where it is totaly empty
        while table_borders.upper_row >= 0:
            if block_detector.is_empty_row(tags[table_borders.upper_row][table_borders.left_col:table_borders.right_col]):
                break
            table_borders.upper_row -= 1

        while table_borders.lower_row < len(tags):
            if block_detector.is_empty_row(tags[table_borders.lower_row][table_borders.left_col:table_borders.right_col]):
                break
            table_borders.lower_row += 1

        while table_borders.left_col >= 0:
            if block_detector.is_empty_col(tags, table_borders.upper_row, table_borders.lower_row, table_borders.left_col):
                break
            table_borders.left_col -= 1

        while True:
            if block_detector.is_empty_col(tags, table_borders.upper_row, table_borders.lower_row, table_borders.right_col):
                break
            table_borders.right_col += 1

        table_borders.upper_row += 1
        table_borders.left_col += 1
        self.borders = table_borders

    # Find label header if it is present in table
    def find_label_names(self, tags):
        if self.time_orientation == utility.row_orientation:
            label_row_num = self.time_block.upper_row
            unknown_label_counter = 0
            for x in self.labels:
                for i in range(x.left_col, x.right_col):
                    if utility.text_cell in tags[label_row_num][i]:
                        label_name = self.parent_sheet.raw_values[label_row_num, i]
                        self.label_names[i] = "_".join(utility.data_to_string(label_name).split(" ")).lower()
                    else:
                        #TODO: Search for label
                        suffix = ""
                        if unknown_label_counter != 0:
                            suffix = "_" + str(unknown_label_counter)
                        self.label_names[i] = "label" + suffix
                        unknown_label_counter += 1
                    self.label_backfill_bool[i] = self.is_merged_label(i, x.upper_row, x.lower_row)
                        
        else:
            label_counter = 0
            for x in self.labels:
                for i in range(x.upper_row, x.lower_row):
                    suffix = ""
                    if label_counter != 0:
                        suffix = "_" + str(label_counter)

                    self.label_names[i] = "label" + suffix
                    label_counter += 1

                    self.label_backfill_bool[i] = self.is_merged_label(i, x.left_col, x.right_col)

    #Check if label block has multiple cells merged together
    def is_merged_label(self, row, col_start, col_end):
        if self.time_orientation == utility.row_orientation:
            for i in range(col_start, col_end):
                if self.parent_sheet.get_merged_block(i, row):
                    return True
            return False
        else:
            for i in range(col_start, col_end):
                if self.parent_sheet.get_merged_block(row, i):
                    return True
            return False

    def create_json(self):
        time_serieses = []
        logging.info("BREAK POINTS: " + str(self.break_points))

        for point in range(len(self.break_points)-1):
            # Stores the set of labels which are already written to the json.
            # Used here to avoid repetition
            seen_labels = {}

            time_series_region_json = dict()
            time_series_region_json['orientation'] = self.time_orientation
            time_series_region_json['metadata'] = []

            logging.info("TIME BLOCK: " + str(self.time_block))

            if self.time_orientation == utility.row_orientation:
                if self.offset == 0:
                    time_series_region_json['locs'] = parsed_table.str_representation(self.time_block.left_col,
                                                                              self.time_block.right_col,
                                                                            utility.column_orientation)
                else:
                    time_series_region_json['locs'] = parsed_table.str_representation_offset(self.time_block.left_col,
                                                                                      self.time_block.right_col,
                                                                                      utility.column_orientation, self.offset+1)
                for x in self.labels:
                    logging.info("LABEL: " + str(x))
                    for i in range(x.left_col, x.right_col):
                        if i in seen_labels:
                            continue
                        else:
                            seen_labels[i] = True

                        meta = {"source": utility.column_orientation, "name": self.label_names[i],"loc": parsed_table.str_representation(i, i + 1, utility.column_orientation)}
                        if self.label_backfill_bool[i] == True:
                            meta["mode"] = "backfill"
                        time_series_region_json['metadata'].append(meta)

                time_series_region_json['times'] = {
                    "locs": parsed_table.str_representation(self.time_block.upper_row, self.time_block.lower_row,utility.row_orientation),"granularity": self.granularity}

                logging.error("time series region" + str(self.data_start) + "' " + str(self.data_end))
            else:
                if self.offset == 0:
                    time_series_region_json['locs'] = parsed_table.str_representation(self.break_points[point],self.break_points[point+1],utility.row_orientation)
                else:
                    time_series_region_json['locs'] = parsed_table.str_representation_offset(self.break_points[point],
                                                                                      self.break_points[point + 1],
                                                                                      utility.row_orientation, self.offset+1)

                for x in self.labels:
                    logging.info("LABEL: " + str(x))
                    for i in range(x.upper_row, x.lower_row):
                        if i in seen_labels:
                            continue
                        else:
                            seen_labels[i] = True

                        meta = {"source": utility.row_orientation, "name": self.label_names[i],"loc": parsed_table.str_representation(i, i + 1, utility.row_orientation)}
                        if self.label_backfill_bool[i] == True:
                            meta["mode"] = "backfill"
                        time_series_region_json['metadata'].append(meta)
                # if self.offset == 0:
                time_series_region_json['times'] = {
                "locs": parsed_table.str_representation(self.time_block.left_col, self.time_block.right_col,utility.column_orientation),
                "granularity": self.find_bounded_granularity(self.break_points[point], self.break_points[point+1])}
                # else:
                #     time_series_region_json['times'] = {
                #         "locs": parsed_table.str_representation_offset(self.time_block.left_col, self.time_block.right_col,
                #                                                 utility.column_orientation, self.offset+1),
                #         "granularity": self.find_bounded_granularity(self.break_points[point],
                #                                                      self.break_points[point + 1])}
            if self.back_mode_time == True:
                time_series_region_json['times']['back_fill_mode'] = True
            logging.error("time series region" + str(self.data_start) + "' " + str(self.data_end))
            time_series_region_json[self.time_orientation + 's'] = parsed_table.str_representation(self.data_start,self.data_end,self.time_orientation)

            if len(time_series_region_json['metadata']) >= 15:
                logging.error("Time series region: ", time_series_region_json[self.time_orientation + 's'], " ", time_series_region_json['locs'], " is likely not valid.")
                continue
            time_serieses.append(time_series_region_json)

        return time_serieses

    def find_time_block_length(self):
        time_block = self.time_block
        time_header_length = max(time_block.lower_row - time_block.upper_row, time_block.right_col - time_block.left_col)
        return time_header_length

    # string representation of the given interval
    @classmethod
    def str_representation(cls, start, end, orientation):
        interval = "["
        if orientation == utility.row_orientation:
            if start+1 == end:
                interval += str(start+1) + "]"
            else:
                interval = interval + str(start+1) + ":" + str(end) + "]"
        else:
            if start+1 == end:
                interval += parsed_table.get_excel_column_name(start) + "]"
            else:
                interval = interval + parsed_table.get_excel_column_name(start) + ":" + parsed_table.get_excel_column_name(end-1) + "]"

        return interval
    @classmethod
    def str_representation_offset(cls, start, end, orientation, offset):
        interval = "["
        if orientation == utility.row_orientation:
            if start+1 == end:
                interval += str(start+1) + "]"
            else:
                interval = interval + str(start+1) + ":" +str(offset)+":"+ str(end) + "]"
        else:
            if start+1 == end:
                interval += parsed_table.get_excel_column_name(start) + "]"
            else:
                interval = interval + parsed_table.get_excel_column_name(start) + ":" +str(offset)+":"+ parsed_table.get_excel_column_name(end-1) + "]"
        return interval

    @classmethod
    def get_excel_column_name(cls, idx):
        if idx <= 25:
            return chr(ord('A') + idx)
        return parsed_table.get_excel_column_name(idx/26 - 1) + chr(ord('A') + idx%26)

    @classmethod
    def is_valid_time_header(self, time_block):
        if time_block.upper_row + 1 == time_block.lower_row and time_block.left_col + 1 == time_block.right_col:
            return False
        if time_block.upper_row + 1 > time_block.lower_row or time_block.left_col + 1 > time_block.right_col:
            logging.warn("Invalid time header skipping")
            return False
        return True

    #Check if two blocks overlap each other
    def is_overlapping(self, table_borders, date_block):
        if date_block.left_col >= table_borders.right_col:
            return False
        if date_block.right_col <= table_borders.left_col:
            return False
        if date_block.upper_row >= table_borders.lower_row:
            return False
        if date_block.lower_row <= table_borders.upper_row:
            return False

        return True

    def date_alignment_measure(self, table):
        if self.time_orientation != table.get_orientation():
            return 0

        old_start = old_end = new_start = new_end = -1
        if self.time_orientation == utility.row_orientation:
            old_start = table.time_block.left_col
            old_end   = table.time_block.right_col
            new_start = self.time_block.left_col
            new_end   = self.time_block.right_col
        else:
            old_start = table.time_block.upper_row
            old_end   = table.time_block.lower_row
            new_start = self.time_block.upper_row
            new_end   = self.time_block.lower_row

        # Computing score similar to F1 score for alignment
        # KNOWN TIME BLOCK:    |-----------|-----------------|                |
        # UNKNOWN TIME BLOCK:  |     FN    |-------TP--------|-------FP-------|
        # FN - False Negative
        # TP - True Positive
        # FP - False Positive

        TP = min(old_end, new_end) - max(old_start, new_start)
        FP = max(0, new_end - old_end) + max(0, old_start - new_start)
        FN = max(0, old_end - new_end) + max(0, new_start - old_start)

        P = float(TP)/(TP + FP)
        R = float(TP)/(TP + FN)

        if P + R != 0:
            score = 2 * P * R / (P + R)
        else:
            score = 0

        return score

    #Validate time block
    def is_valid_time_header_post(self, known_tables):
        #Heuristics to check whether series of numbers is a date
        #H1: Date block has a different orientation from previous date blocks
        #H2: Date block is shorter compared to previous blocks
        
        #H3: Check surroundings of date blocks, position in table
        #Case 1: Date block is outside of previously detected tables => probably a date
        #Case 2: dates are aligned but inside previous table => multiple tables are merged to form a single table structure
        #Case 3: dates are not aligned and inside previous table => It's not actually a date.
        
        different_orientation = True
        short_time_block = True
        table_overlap = False
        max_date_alignment_measure = 0

        if len(known_tables) == 0:
            different_orientation = False
            short_time_block = False
        
        for table in known_tables:
            #H1
            if self.time_orientation == table.get_orientation():
                different_orientation = False
            #H3
            if self.is_overlapping(table.borders, self.time_block):
                table_overlap = True
            date_alignment_measure = self.date_alignment_measure(table)
            max_date_alignment_measure = max(max_date_alignment_measure, date_alignment_measure)

        #H2
        if self.time_block_length >= 5:
            short_time_block = False
        else:
            for table in known_tables:
                if table.time_block_length < 5:
                    short_time_block = False

        if table_overlap == False:
            return True
        elif date_alignment_measure >= 0.666:
            logging.info("Previous table border may have been computed incorrectly")
            return True
        elif different_orientation == True:
            return False
        elif short_time_block == True:
            logging.info("Highly unlikely to be a time block, but not sure")
            return False
        else:
            logging.info("Unexpected date block found.")
            return True
