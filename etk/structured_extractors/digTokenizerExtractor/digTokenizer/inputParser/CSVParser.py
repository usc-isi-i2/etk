#!/usr/bin/env python

class CSVParser(object):

    def __init__(self, config, **options):
        pass

    def parse(self, values):
        """line is list of values
returns (<first value>, <all remaining values in list>)"""
        return (values[0], values[1:])

    def parse_values(self, values):
        """returns all values"""
        return values
