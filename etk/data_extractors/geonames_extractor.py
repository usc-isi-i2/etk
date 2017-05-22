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


def get_country_from_states(states, state_to_country_dict):
    countries = list()
    for state in states:
        if state in state_to_country_dict:
            country = state_to_country_dict[state]
            for cty in country:
	            countries.append({"value": cty})

    return countries


def get_country_from_populated_places(populated_places):
    countries = list()
    for place in populated_places:
        country = place['metadata']['country']
        if country not in countries:
            countries.append({"value": country})

    return countries
