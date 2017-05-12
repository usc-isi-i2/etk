# -*- coding: utf-8 -*-
import codecs
import json


def get_populated_places(cities, geonames_dict):
    populated_places = list()
    for city in cities:
        if city in geonames_dict:
            geo_cities = geonames_dict[city]
            for geo_city in geo_cities:
                place = dict()
                place['value'] = city
                place['metadata'] = geo_city
                # place['context'] = dict()
                if place not in populated_places:
                    populated_places.append(place)

    return populated_places
