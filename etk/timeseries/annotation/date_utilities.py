import datetime
import logging
import re

months = [ 'jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov','dec', 'january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september',
            'october', 'november', 'december']
exact_time = {"jan":1, "feb":2, "mar":3, "apr":4, "may":5, "jun":6, "jul":7, "aug":8, "sep":9, "oct":10, "nov":11, "dec":12,
            "january":1, "february":2, "march":3, "april":4, "june":6, "july":7, "august":8, "september":9, "october":10, "november":11, "december":12,
              }

special_keywords = [ 'jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov',
                    'dec', 'january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september',
                    'october', 'november', 'december', 'q1', 'q2', 'q3', 'q4']

date_parts = {"%Y":'year', "%B":'month', "%d": 'day'}

# a date point
class parsed_date:
    def __init__(self):
        self.is_range = False
        self.predicted_date = dict()

    def get_printable_version(self):
        return self.predicted_date

    def equals(self, p):
        for i in self.predicted_date.keys():
            if i not in p.predicted_date.keys():
                return False
            if p.predicted_date[i] != self.predicted_date[i]:
                return False
        return True

class parsed_interval:
    def __init__(self):
        self.begin_interval = parsed_date()
        self.end_interval = parsed_date()
        self.predicted_date_certainty = dict()

    def equals(self, p):
        if not self.begin_interval.equals(p.begin_interval):
            return False
        if not self.end_interval.equals(p.end_interval):
            return False
        return True
    def print_interval(self):
        stres = "begin: " + str(self.begin_interval.get_printable_version()) +  " end: " + str(self.end_interval.get_printable_version())
        print(stres)

# direction = True means the first element in year, direction False means the last element is last
def parse_pattern(cell, pattern , direction, day_available):
    guessed_date = parsed_interval()
    matchObj = re.match(pattern, cell, re.M | re.I)

    dir = [1, 2, 3]
    guessed_date.begin_interval.predicted_date['year'] = int(matchObj.group(dir[0]))
    guessed_date.end_interval.predicted_date['year'] = int(matchObj.group(dir[0]))
    guessed_date.predicted_date_certainty['year'] = 1
    guessed_date.begin_interval.predicted_date['month'] = int(matchObj.group(dir[1]))
    guessed_date.end_interval.predicted_date['month'] = int(matchObj.group(dir[1]))
    guessed_date.predicted_date_certainty['month'] = 1


    if day_available:
        guessed_date.begin_interval.predicted_date['day'] = int(matchObj.group(3))
        guessed_date.end_interval.predicted_date['day'] = int(matchObj.group(3))
        guessed_date.predicted_date_certainty['day'] = 1
    else:
        guessed_date.begin_interval.predicted_date['day'] = 1
        guessed_date.end_interval.predicted_date['day'] = 30
        guessed_date.predicted_date_certainty['day'] = 0.5


def parse_date(cell):
    guessed_date = parsed_interval()
    if type(cell) == datetime.date or type(cell) == datetime.datetime:
        guessed_date.begin_interval.predicted_date[date_parts["%Y"]]= int(cell.strftime("%Y")) # this is the year
        guessed_date.end_interval.predicted_date[date_parts["%Y"]] = int(cell.strftime("%Y"))
        guessed_date.predicted_date_certainty[date_parts["%Y"]] = 1

        guessed_date.begin_interval.predicted_date[date_parts["%B"]] = int(exact_time[cell.strftime("%B").lower()])
        guessed_date.end_interval.predicted_date[date_parts["%B"]] = int(exact_time[cell.strftime("%B").lower()])
        guessed_date.predicted_date_certainty[date_parts["%B"]] = 1


        guessed_date.begin_interval.predicted_date[date_parts["%d"]] = int(cell.strftime("%d"))
        guessed_date.end_interval.predicted_date[date_parts["%d"]] = int(cell.strftime("%d"))
        if int(cell.strftime("%d")) == 1: # not working dont know why
            guessed_date.predicted_date_certainty[date_parts["%d"]] = 0.5
            guessed_date.end_interval.predicted_date[date_parts["%d"]] = 30

            if exact_time[cell.strftime("%B").lower()] == 1:  # if it was not numerical
                guessed_date.predicted_date_certainty[date_parts["%B"]] = 0.5
                guessed_date.end_interval.predicted_date[date_parts["%B"]] = 12

        else:
            guessed_date.predicted_date_certainty[date_parts["%d"]] = 1
        #logging.info("")
             # this is day -> what if the day is not indicated? maybe this is not valid... I should have a bit vector specially when it has the value exactly "1"
        return guessed_date
    # if the type is date and it is a text:
    if type(cell) == int:
        if cell > 1900 and cell < 2020:
            guessed_date.begin_interval.predicted_date['year'] = cell
            guessed_date.end_interval.predicted_date['year'] = cell
            guessed_date.predicted_date_certainty['year'] = 1
    else:

        try:
            # print cell
            if int(cell) > 1900 and int(cell) < 2020: #current year should be added
                guessed_date.begin_interval.predicted_date['year'] = int(cell)
                guessed_date.end_interval.predicted_date['year'] = int(cell)
                guessed_date.predicted_date_certainty['year'] = 1
        except:
            if cell.lower() in exact_time.keys():
                guessed_date.begin_interval.predicted_date['month'] = exact_time[cell.lower()]
                guessed_date.end_interval.predicted_date['month'] = exact_time[cell.lower()]
                guessed_date.predicted_date_certainty['month'] = 1
                return guessed_date
            elif re.match('q\d', cell.lower()):
                guessed_date.begin_interval.predicted_date['month'] = (int(cell[1])-1) * 3 + 1
                guessed_date.begin_interval.predicted_date['day'] = 1
                guessed_date.end_interval.predicted_date['month'] = int(cell[1])*3
                guessed_date.end_interval.predicted_date['day'] = 30
                guessed_date.predicted_date_certainty['month'] = 1
                guessed_date.predicted_date_certainty['day'] = 0.5
            elif len(cell.lower().split('-')) > 1:
                # patterns = [r'(\d+)-(\d+)', r'(\d+)-(\d+)-(\d+)']
                #
                # for x in patterns:
                #      matchObj = re.match(x, cell, re.M | re.I)
                #      if matchObj:
                #          if int(matchObj.group(1)) > 1000:  # this is perhaps the year
                #              if len(cell.lower.split('-')) > 2:
                #                  return parse_pattern(cell, x, True, True)
                #              else:
                #                  return parse_pattern(cell, x, True, False)

                     # complete for the reverse direction

                sub_dates = cell.lower().split('-') # it can be a range or it can be a specific date
                guessed_date.begin_interval = parse_date(sub_dates[0]).begin_interval
                guessed_date.predicted_date_certainty = parse_date(sub_dates[0]).predicted_date_certainty
                guessed_date.end_interval = parse_date(sub_dates[1]).begin_interval
            elif len(cell.lower().split(' ')) > 1:
                sub_dates = cell.lower().split(' ')  # it can be a range or it can be a specific date
                guessed_date.begin_interval = parse_date(sub_dates[0]).begin_interval
                guessed_date.predicted_date_certainty = parse_date(sub_dates[0]).predicted_date_certainty
                guessed_date.end_interval = parse_date(sub_dates[1]).begin_interval
            elif re.match(r'(\d+)[^\d+](\d+)', cell, re.M | re.I):
                matchObj = re.match(r'(\d+)[^\d+](\d+)', cell)
                guessed_date.begin_interval.predicted_date['year'] = int(matchObj.group(1))
                guessed_date.end_interval.predicted_date['year'] = int(matchObj.group(1))
                guessed_date.predicted_date_certainty['year'] = 1
                guessed_date.begin_interval.predicted_date['month'] = int(matchObj.group(2))
                guessed_date.end_interval.predicted_date['month'] = int(matchObj.group(2))
                guessed_date.predicted_date_certainty['month'] = 1

            else:
                logging.error("could not parse this date " + str(cell))
    return guessed_date

# we assume that currently we have daily monthly and yearly --> (quarterly and weekly will be added later
def find_date_intervals_differences(date_range1, date_range2):
    if 'year' in date_range1.begin_interval.predicted_date.keys() and 'year' in date_range2.begin_interval.predicted_date.keys():
        if date_range2.begin_interval.predicted_date['year'] != date_range1.begin_interval.predicted_date['year']:
            return 'yearly'

    if 'month' in date_range1.begin_interval.predicted_date.keys() and 'month' in date_range2.begin_interval.predicted_date.keys():
        if date_range2.begin_interval.predicted_date['month'] != date_range1.begin_interval.predicted_date['month']:
            if abs(date_range2.begin_interval.predicted_date['month'] - date_range1.begin_interval.predicted_date['month']) >= 3:
                return 'quarterly'
            else:
            #    print 'strange' + abs(date_range2.begin_interval.predicted_date['month'] - date_range1.begin_interval.predicted_date['month'])
                return 'monthly' # or maybe quarterly

    if 'day' in date_range1.begin_interval.predicted_date.keys() and 'day' in date_range2.begin_interval.predicted_date.keys():
        if date_range2.begin_interval.predicted_date['day'] != date_range1.begin_interval.predicted_date['day']:
            return 'daily'  # or maybe quarterly

    # add other granularities

    return 'no idea'

# parse the whole row as a unique timee interval
def parse_row_as_date(row):
    if len(row) == 1:
        return parse_date(row[0])

    # should start and shrink the date
    predicted_date = parse_date(row[0])
    for i in range(len(row) - 1):
        pr_date = parse_date(row[i+1])
        for j in pr_date.begin_interval.predicted_date.keys():
            if pr_date.predicted_date_certainty[j] == 1:
                # check the overlapp of two time intervals
                if j in predicted_date.begin_interval.predicted_date.keys():
                    if predicted_date.predicted_date_certainty[j] == 1:
                    # compare the intervals here
                        if predicted_date.begin_interval.predicted_date[j] >= pr_date.begin_interval.predicted_date[j] and predicted_date.end_interval.predicted_date[j] <= pr_date.end_interval.predicted_date[j]:
                            continue
                        # if it is finer then shrink the interval here
                        elif predicted_date.begin_interval.predicted_date[j] <= pr_date.begin_interval.predicted_date[j] and predicted_date.end_interval.predicted_date[j] >= pr_date.end_interval.predicted_date[j] :
                            predicted_date.begin_interval.predicted_date[j] = pr_date.begin_interval.predicted_date[j]
                            predicted_date.end_interval.predicted_date[j] = pr_date.end_interval.predicted_date[j]
                            predicted_date.predicted_date_certainty[j] = 1
                        continue
                else:
                    predicted_date.begin_interval.predicted_date[j] = pr_date.begin_interval.predicted_date[j]
                    predicted_date.end_interval.predicted_date[j] = pr_date.end_interval.predicted_date[j]
                    predicted_date.predicted_date_certainty[j] = pr_date.predicted_date_certainty[j]

    return predicted_date

def find_granularity(rows, left_lim, right_lim):
    founded_gr = {}
    for i in range(len(rows) - 1):
        founded_gr[find_date_intervals_differences(parse_row_as_date(rows[i][left_lim:right_lim]), parse_row_as_date(rows[i + 1][left_lim:right_lim]))]=1
    # print founded_gr
    # return the finest granularity
    if 'daily' in founded_gr:
        return 'daily'
    if 'monthly' in founded_gr:
        return 'monthly'
    if 'quarterly' in founded_gr:
        return 'quarterly'
    if 'yearly' in founded_gr:
        return 'yearly'

    return 'No idea'

#check if the given row can be a real date interval(no conflict between the cells)
def valid_time_row(row):
    predicted_date = parse_date(row[0])
    if len(row) == 1:
        return True, predicted_date

    for i in range(len(row) - 1):
        pr_date = parse_date(row[i+1])
        for j in pr_date.begin_interval.predicted_date.keys():
            if pr_date.predicted_date_certainty[j] == 1:
                # check the overlapp of two time intervals
                if j in predicted_date.begin_interval.predicted_date.keys():
                    if predicted_date.predicted_date_certainty[j] == 1:
                        if predicted_date.begin_interval.predicted_date[j] >= pr_date.begin_interval.predicted_date[j] and predicted_date.end_interval.predicted_date[j] <= pr_date.end_interval.predicted_date[j]:
                            continue
                        # if it is finer then shrink the interval here
                        elif predicted_date.begin_interval.predicted_date[j] <= pr_date.begin_interval.predicted_date[j] and predicted_date.end_interval.predicted_date[j] >= pr_date.end_interval.predicted_date[j] :
                            predicted_date.begin_interval.predicted_date[j] = pr_date.begin_interval.predicted_date[j]
                            predicted_date.end_interval.predicted_date[j] = pr_date.end_interval.predicted_date[j]
                            predicted_date.predicted_date_certainty[j] = 1
                            continue
                        return False, None
                else:
                    predicted_date.begin_interval.predicted_date[j] = pr_date.begin_interval.predicted_date[j]
                    predicted_date.end_interval.predicted_date[j] = pr_date.end_interval.predicted_date[j]
                    predicted_date.predicted_date_certainty[j] = pr_date.predicted_date_certainty[j]

    return True, pr_date

def is_greater(date1, date2):
    key_w = ['year', 'month', 'day']
    for w in key_w:
        if w in date1.end_interval.predicted_date.keys() and w in date2.begin_interval.predicted_date.keys() and date1.predicted_date_certainty[w] == 1 and date2.predicted_date_certainty[w] == 1:
            if date2.begin_interval.predicted_date[w] < date1.end_interval.predicted_date[w]:
                return False
            elif date2.begin_interval.predicted_date[w] > date1.end_interval.predicted_date[w]:
                return True
    return True