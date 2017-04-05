import os
import csv
country_list_ = os.path.join(os.path.dirname(__file__), 'countrylist.csv')

def get_all_country_iso_two_letter_code():
    ht = {}
    with open(country_list_, 'rb') as csvfile:
        csvreader = csv.reader(csvfile)
        csvreader.next()
        for row in csvreader:
            ht.setdefault(row[-4], 0)
    return ht.keys()


if __name__ == '__main__':
    print get_all_country_iso_two_letter_code() 