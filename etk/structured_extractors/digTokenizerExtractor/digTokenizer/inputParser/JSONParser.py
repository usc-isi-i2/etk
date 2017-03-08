#!/usr/bin/env python

import json

class JSONParser(object):

    def __init__(self, paths, **options):
        """
        :param paths: these are json paths of the fields
        :param options:
        :return:
        JSONParse takes paths of fields as argument
        """
        self.column_paths = paths
        self.key_path = 'uri'

    def parse(self, data):
        parsed = self.parse_with_paths(data, self.key_path, self.column_paths)
        #print "\n\n\nReturn:", parsed[0], "value:", parsed[1]
        return (parsed[0], parsed[1])

    def parse_values(self, data):
        return self.parse_values_with_paths(data, self.column_paths)

    def parse_with_key(self, x, key_name):
        return self.parse_values_with_paths(x, key_name)

    def parse_with_paths(self, json_data, key_name, paths):
        key = json_data[key_name]
        value = self.__extract_columns(json_data, paths)
        return key, value

    def parse_values_with_paths(self, json_data, paths):
        vals = self.__extract_columns(json_data, paths)
        #print '\n\n input data is'
        #print json_data
        #print '\n\n\n fields are'
        #print vals
        return vals

    def __extract_columns(self, row, paths):
        fields = []
        for path in paths:
            x = list(self.__extract_at_path(row, path))
            x = [' '.join(x)]
            fields.append(x)
        return fields

    def __extract_at_path(self, row, path):
        start = self.to_list(row)
        found = True
        path_elems = path.split(".")
        for path_elem in path_elems:
            if not isinstance(start, list):
                break
            start = self.__extract_elements(start, path_elem)
            if len(start) == 0:
                found = False
                break


        #print "FOUND:", found, ":", start
        if found:
            if isinstance(start, list):
                for elem in start:
                   yield elem

            else:
                yield start

    def __extract_elements(self, array, elem_name):
        result = []
        for elem in array:
            if elem_name in elem:
                #print "Elem:", elem, "elem_name:", elem_name
                elem_part = elem[elem_name]
                if isinstance(elem_part, list):
                    result.extend(elem_part)
                else:
                    result.append(elem_part)

        #print "\n\nFind ", elem_name, "in", array, "\nResult:", result
        return result


    def to_list(self, some_object):
        if not isinstance(some_object, list):
            arr = list()
            arr.append(some_object)
            return arr
        return some_object
