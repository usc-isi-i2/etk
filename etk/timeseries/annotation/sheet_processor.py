from etk.timeseries.annotation import utility, block_detector
from etk.timeseries.annotation.table_processor import parsed_table
from etk.timeseries.annotation import cell_classifier
import logging
from etk.timeseries.annotation import multi_table_processor

class parsed_sheet:
    def __init__(self, name, index, real_values, merged_cells):
        self.sheet_name = name
        self.sheet_index = index
        self.raw_values = real_values
        self.classifier = cell_classifier.simple_tag_classifier()
        self.add_tags()
        self.table_list = []
        self.merged_cells = merged_cells
        self.in_complex = False


    # Returns the merged block if the given cell is located in one.
    def get_merged_block(self, row, col):
        for block in self.merged_cells:
            if row >= block[0] and row < block[1] and col >= block[2] and col < block[3]:
                return block
        return None

    # looks for the neighbor cells of the given date_cell and returns the the smallest rectangular block that this date is located in.
    def find_date_blocks(self, date_cell):
        blocks = []
        for i in range(len(self.classified_tags)):
            for j in range(len(self.classified_tags[i])):
                if date_cell not in self.classified_tags[i][j]:
                    continue
                if block_detector.check_cell_in_block(blocks, i, j):
                    continue
                blocks.append(block_detector.BFS_date_block(i, j, self.classified_tags, date_cell))

        for block in blocks:
            logging.debug("Date block: " + str(block))
            yield block_detector.find_minimum_boundry_rectangle(block)

    # finding the tables with time series in the given sheet
    def find_tables(self, out_file):
        step = 1
        sheet_label_blocks = []
        logging.info("------------------------------------")
        for rb in self.find_date_blocks(self.classifier.get_date_tag()):
            # check if the header is only a single time cell (which can be an event in the best case)

            if not parsed_table.is_valid_time_header(rb):
                print("Pre validation of time header failed. " + str(rb))
                continue

            detected_table = parsed_table(rb, self)
            if not detected_table.is_valid_time_header_post(self.table_list):
                print("Post validation of time header failed. " + str(rb))
                continue

            sheet_label_blocks = []
            step += 1
            orientation = detected_table.get_orientation()

            print("Date block detected : " + str(rb))
            logging.info("Orientation is " + orientation)

            # if they are merged time cells
            for i in range(rb.upper_row, rb.lower_row):
                for j in range(rb.left_col, rb.right_col):
                    if self.get_merged_block(i, j) != None:
                        detected_table.check_offset()

            # check for overlapping tables caused by overlapping times
            has_overlap, break_points = multi_table_processor.complex_table.check_for_overlapping_time(detected_table.time_block, orientation, self.raw_values)
            detected_table.break_points = break_points
            if has_overlap:
                detected_table.is_complex = True

            if orientation == utility.row_orientation:
                for x in block_detector.find_label_block(self.get_tags(), detected_table.data_start, rb.lower_row, 0, rb.left_col, utility.column_key, sheet_label_blocks):
                    detected_table.add_label(x)
            else:

                for x in block_detector.find_label_block(self.get_tags(), detected_table.data_start, rb.right_col, 0, rb.upper_row, utility.row_key, sheet_label_blocks):
                    out_file.write('rows[' + str(x.upper_row + 1) + "-" + str(x.lower_row) + "] columns[ " + str( x.left_col + 1) + "-" + str(x.right_col) + "]\n")

                    detected_table.add_label(x)

            detected_table.find_table_borders(self.get_tags())
            logging.info("Table borders: " + str(detected_table.borders))

            # check for mergeds cells aftar finding the table borders
            # check for the remaining cells that are not present in any block. They may be labels?
            # check for unassigned_cells

            dtb = detected_table.borders
            #Find more labels on the right/bottom side of date blocks
            if orientation == utility.row_orientation:
                for x in block_detector.find_label_block(self.get_tags(), detected_table.data_start, rb.lower_row, rb.right_col, dtb.right_col, utility.column_key, sheet_label_blocks):
                    detected_table.add_label(x)
                    out_file.write('rows[' + str(x.upper_row + 1) + "-" + str(x.lower_row) + "] columns[ " + str(x.left_col + 1) + "-" + str(x.right_col) + "]\n")
            else:
                for x in block_detector.find_label_block(self.get_tags(), detected_table.data_start, rb.right_col, rb.lower_row, dtb.lower_row, utility.row_key, sheet_label_blocks):
                    out_file.write('rows[' + str(x.upper_row + 1) + "-" + str(x.lower_row) + "] columns[ " + str( x.left_col + 1) + "-" + str(x.right_col) + "]\n")
                    detected_table.add_label(x)
            detected_table.find_label_names(self.get_tags())

            self.add_table(detected_table)

    def add_table(self, table):
        self.table_list.append(table)


    def add_tags(self):
        self.classified_tags = self.classifier.tag_sheet(self.raw_values)


    # get class tags of the sheet cells
    def get_tags(self):
        return self.classified_tags

    # creates the output json for the sheet based on the tables found
    def get_output(self):
        if len(self.table_list) == 0:
            return None
        sheet_json = dict()
        property = {"sheet_indices": str([self.sheet_index])}
        sheet_json['Properties'] = property
        sheet_json['TimeSeriesRegions'] = []
        for table in self.table_list:
            for t_json in table.create_json():
                sheet_json['TimeSeriesRegions'].append(t_json)
        sheet_json['GlobalMetadata'] = [{"name": "title", "source": "sheet_name"}]

        return sheet_json

    @classmethod
    def merge_sheet_indices(cls, idx1, idx2):
        idx1_stripped = idx1[1:-1].split(":")
        idx2_stripped = str(idx2[1:-1])

        if idx1 == idx2:
            return idx1
        elif len(idx1_stripped) == 2 and int(idx1_stripped[1]) + 1 == int(idx2_stripped):
            return "[" + idx1_stripped[0] + ":" + idx2_stripped + "]"
        elif len(idx1_stripped) == 1 and int(idx1_stripped[0]) + 1 == int(idx2_stripped):
            return "[" + idx1_stripped[0] + ":" + idx2_stripped + "]"
        else:
            raise Exception("Sheet indices cannot be merged.")

    @classmethod
    def merge_row_indices(cls, idx1, idx2):
        if idx1 == idx2:
            return idx1
        else:
            raise Exception("Row indices cannot be merged.")

    @classmethod
    #Merge json for 2 sheets if they are similar
    def merge_json(cls, json1, json2):
        try:
            if type(json1) != type(json2):
                raise Exception("Json type is not equal")
            if isinstance(json1, list):
                ret = []
                if len(json1) != len(json2):
                    raise Exception("List length not equal")
                for item1, item2 in zip(json1, json2):
                    merged = cls.merge_json(item1, item2)
                    ret.append(merged)
                return ret
            elif isinstance(json1, dict):
                ret = {}
                if len(json1) != len(json2):
                    raise Exception("Dict length not equal.")
                for key in json1:
                    if key in json2:
                        if type(json1[key]) == type(json2[key]):
                            if isinstance(json1[key], list) or isinstance(json1[key], dict):
                                merged = cls.merge_json(json1[key], json2[key])
                                ret[key] = merged
                            elif key == "sheet_indices":
                                ret[key] = cls.merge_sheet_indices(json1[key], json2[key])
                            elif key == "rows":
                                ret[key] = cls.merge_row_indices(json1[key], json2[key])
                            elif json1[key] == json2[key]:
                                ret[key] = json1[key]
                            else:
                                raise Exception("Key values are not equal.")
                        else:
                            raise Exception("Dict key types do not match")
                    else:
                        raise Exception("Key not found in dict")
                return ret
            elif json1 == json2:
                return json1
        
        except Exception as exc:
            raise Exception(exc)

    @classmethod
    def merge_json_list(cls, final_json):
        return_json = []
        json = final_json[0]
        for i in range(len(final_json)):
            try:
                merged_json = cls.merge_json(json, final_json[i])
                json = merged_json
            except:
                return_json.append(json)
                json = final_json[i]

        return_json.append(json)

        return return_json
