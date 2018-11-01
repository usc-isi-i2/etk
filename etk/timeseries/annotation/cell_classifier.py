import datetime
import string
from etk.timeseries.annotation.utility import date_cell, number_cell, text_cell, empty_cell, blank_cell, special_keywords
import logging
import re

class simple_tag_classifier:

    def get_date_tag(self):
        return date_cell

    def tag_sheet(self, raw_sheet):
        tags = []
        i = 0
        for row in raw_sheet.rows():
            tags.append([])
            for col in row:
                tags[i].append(self.find_class(col))
            i += 1
        return tags

    def find_class(self, cell):
            if type(cell) == datetime.date:
                return {date_cell}
            if type(cell) == datetime.datetime:
                return {date_cell}
            if type(cell) == float:
                return {number_cell}
            if type(cell) == int or type(cell) == long:
                if cell < 2020 and cell > 1900:
                    return {date_cell, number_cell}
                else:
                    return {number_cell, text_cell}
            if type(cell) == str or type(cell) == unicode:
                if cell == '-':
                    return [blank_cell]
                if len(string.replace(cell, ' ', '')) == 0:
                    return {empty_cell}
                if self.possible_date_presentation(cell):
                    return {date_cell, text_cell}
                float_flag = False
                try:
                    a = float(cell)
                    float_flag = True
                    a = int(cell)
                    return {number_cell, text_cell}
                except:
                    if float_flag:
                        return {number_cell}
                return {text_cell}
            logging.error("wow this cell has no tag!!!!!! " + str(cell) + " " + type(cell))
            
    def possible_date_presentation(self, cell):
        try:
            x = int(cell)
            if x > 1900 and x <= 2020:  # I should add the current date parameter
                return True
        except:
            for word in special_keywords:
                if cell.lower().strip() == word:
                    return True

            cell_modified = string.replace(cell, '-', '/')
            cell_modified = string.replace(cell_modified, '.', '/')
            cell_modified = string.replace(cell_modified, '/', ' ')

            #    print splitted_cell
            if len(string.replace(cell_modified, ' ', '')) == 0:
                return False

            splitted_cell = (cell_modified.strip()).split()

            if len(splitted_cell) > 3:
                return False
            if len(splitted_cell) == 1:

                matchObj = re.match(r'(\d{4})[^\d].*', cell, re.M | re.I)
                if matchObj:
                    # print cell + 'matched'
                    if int(matchObj.group(1)) > 1900 and int(matchObj.group(1))<2020:
                        return True
                matchObj = re.match(r'.*[^\d](\d{4})', cell, re.M | re.I)
                if matchObj:
                    if int(matchObj.group(1)) > 1900 and int(matchObj.group(1))<2020:
                        return True
                # print 'not date: ' + cell
                return False

            for part in splitted_cell:
                if part.lower() in special_keywords:
                    continue
                try:
                    x = int(part)
                    if x == 0:
                        return False
                    if x >= 1900 and x < 2020:
                        continue
                    if x <= 31:
                        continue
                    else:
                        return False
                except:
                    return False
            return True