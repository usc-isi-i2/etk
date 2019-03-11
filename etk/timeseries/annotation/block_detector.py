from etk.timeseries.annotation.utility import date_cell, text_cell, empty_cell, blank_cell, number_cell, row_key, column_key, column_orientation
import logging

class rectangular_block:
    def __init__(self):
        self.upper_row = 0
        self.lower_row = 0
        self.left_col = 0
        self.right_col = 0

    def __str__(self):
        attrs = vars(self)
        return ', '.join("%s: %s" % item for item in attrs.items())

def find_label_block(tags, start_break, row_lim, col_start, col_end, orientation, detected_blocks):
    blocks = []
    tag_user = tags

    if orientation == row_key:
        #Transpose tag matrix
        tag_user = list(map(list, zip(*tags)))
        for rb in find_label_block(tag_user, start_break, row_lim, col_start, col_end, column_key, detected_blocks):
            t1, t2 = rb.upper_row, rb.lower_row
            rb.upper_row, rb.lower_row = rb.left_col, rb.right_col
            rb.left_col, rb.right_col = t1, t2
            yield rb
    else:
        curr_row = row_lim
        while curr_row < start_break or (curr_row < len(tags) and is_empty_label_row(tags[curr_row], col_end)):
            curr_row += 1
        # edit here to first go and look for the first non empty row that matches...
        while curr_row < len(tags) and not is_empty_label_row(tags[curr_row], col_end):
            curr_row += 1
            #Should we check till start of sheet?
            for col in range(col_end - 1, col_start - 1, -1): # should n't I only keep the closest?
                #Note: Removing this as labels are not necessarily text. Run against dataset and check TODO
                #if text_cell not in tags[curr_row-1][col]:
                #    continue
                if empty_cell in tags[curr_row - 1][col]:
                    continue
                if check_cell_in_block(detected_blocks, curr_row-1, col):
                    continue
                if check_cell_in_block(blocks, curr_row-1, col):
                    continue

                blocks.append(BFS_limited_block(curr_row-1, col, tags, row_lim, col_start, col_end))
    for block in blocks:
        minimum_boundary_rect = find_minimum_boundry_rectangle(block)
        logging.info("Label Block: " + str(minimum_boundary_rect))
        detected_blocks.append(block)
        yield minimum_boundary_rect


def is_empty_label_row(tags, col_lim):
    for i in range(col_lim):
        if empty_cell not in tags[i]:
            return False
        else: # or maybe analyze the meaning of the text in order to find out if it is the end or not
            continue
    return True


def BFS_limited_block(row, col, tags, row_lim, col_start, col_end):
    Q = []
    Q.append((row, col))
    block = []
    directions = [(0, 1), (0, -1), (1, 0), (-1, 0)]
    while len(Q) > 0:
        front = Q[0]
        block.append(Q.pop(0))
        # check the neighbors
        for dir in directions:
            if front[1] + dir[1] < len(tags[front[0]]) and front[1] + dir[1] < col_end and front[1] + dir[1] >= col_start and front[0] + dir[0] >= row_lim and front[0] + dir[0] < len(tags) and front[0] + dir[0] >= 0 and front[1] + dir[1] >= 0:
                #Removing check for text cell
                if empty_cell not in tags[front[0] + dir[0]][front[1] + dir[1]]:
                    if (front[0] + dir[0], front[1] + dir[1]) not in Q and (
                        front[0] + dir[0], front[1] + dir[1]) not in block:
                        Q.append((front[0] + dir[0], front[1] + dir[1]))

    return block


def find_data_block(a, tags, time_header_begin_row, time_header_end_row, time_header_begin_col, time_header_end_col, time_series_orientation):
    if time_series_orientation == column_orientation:
        #Transpose tag matrix
        tag_user = map(list, zip(*tags))
        return find_data_block(a, tag_user, time_header_begin_col, time_header_end_col, time_header_begin_row, time_header_end_row, row_key)
    else:
        # find the first non-empty row (should I add containing only numbers?)

        start_row = time_header_end_row
        for i in range(time_header_end_row, len(tags)): # continue untill reaching the first non empty row
            if is_a_time_series_row(tags[i], time_header_begin_col, time_header_end_col):
                start_row = i
                break

        j = start_row
        for j in range(start_row, len(tags)):
            if is_a_time_series_row(tags[j], time_header_begin_col, time_header_end_col):
                continue

            return start_row, j
        return start_row, j


def find_minimum_boundry_rectangle(block):
    first_cell = block[0]
    rb = rectangular_block()
    min_row = first_cell[0]
    min_col = first_cell[1]
    max_row = min_row
    max_col = min_col
    for cell in block:
        if cell[0] < min_row:
            min_row = cell[0]
        if cell[0] > max_row:
            max_row = cell[0]
        if cell[1] < min_col:
            min_col = cell[1]
        if cell[1] > max_col:
            max_col = cell[1]
    rb.left_col = min_col
    rb.right_col = max_col+1
    rb.upper_row = min_row
    rb.lower_row = max_row + 1
    return rb


def BFS_date_block(row, col, tags, desired_tag):
    logging.info("Starting BFS from [%d, %d]", row, col)
    Q = []
    Q.append((row, col))
    block = []
    directions = [(0,1), (0, -1), (1, 0), (-1, 0)]
    while len(Q) > 0:
        front = Q[0]
        block.append(Q.pop(0))
        # check the neighbors
        for dir in directions:
            if front[1] + dir[1] < len(tags[front[0]]) and front[0]+dir[0] < len(tags) and front[0]+dir[0] >= 0 and front[1] + dir[1] >= 0:
                if desired_tag in tags[front[0]+dir[0]][front[1]+dir[1]]:
                    if (front[0]+dir[0], front[1]+dir[1]) not in Q and (front[0]+dir[0], front[1]+dir[1]) not in block:
                        Q.append((front[0]+dir[0], front[1]+dir[1]))
    logging.info(block)
    return block

def check_in_merged_block(blocks, row, col):
    for block in blocks:
        if row >= block[0] and row < block[1] and col >= block[2] and col < block[3]:
            return True
    return False


def check_cell_in_block(blocks, row, col):
    for block in blocks:
        for cell in block:
            if cell[0] == row and cell[1] == col:
                return True
    return False


def is_a_time_series_row(row, left_col, right_col):
    seen_tags = row[left_col:right_col]
    seen_number = False

    # if number cell not available or cells are empty
    for x in seen_tags:
        if x == {text_cell} or x == {date_cell}:
            return False
        if number_cell in x:
            seen_number = True
            continue
        if blank_cell in x:
            seen_number = True
            continue
        if empty_cell in x:
            continue

    if not seen_number:
        return False
    return True

def is_empty_row(row_tags):
    for tag in row_tags:
        if empty_cell not in tag:
            return False
    return True


def is_empty_col(tags, start_row, end_row, col):
    for row in range(start_row, end_row):
        if col >= len(tags[row]):
            continue
        if empty_cell not in tags[row][col]:
            return False
    return True

def find_closest_date_cell(tags, row, col, borders):
    while row > borders.upper_row:
        row -= 1
        if date_cell in tags[row][col]:
            return row, col
    while col > borders.left_col:
        col -= 1
        if date_cell in tags[row][col]:
            return row, col

