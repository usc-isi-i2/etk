#!/usr/bin/env python

from JSONParser import JSONParser

class ParserFactory:
    def __init__(self):
        pass

    @staticmethod
    def get_parser(config, text_format=None, **kwargs):
        column_paths = list()
        for index in config["fieldConfig"]:
            if "path" in config["fieldConfig"][index]:
                column_paths.insert(int(index), config["fieldConfig"][index]["path"])
        parser = JSONParser(column_paths, **kwargs)
        return parser
