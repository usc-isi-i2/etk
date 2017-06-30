import json
import math


def country_next_to_city(knowledge_graph, given_country):
    """ Distance between the country token and the city token.
    If the country token doesn't exist, result is 0. Else, if it exists,
    it is the min distance between end of all cities and start of country"""
    result = 0
    found_country = False
    country_start = dict()  # dict of segment: start token val

    country_prov = knowledge_graph["country"][given_country]
    for prov in country_prov:
        if "origin" in prov:
            found_country = True
            country_start[prov["origin"]["segment"]] = prov["context"]["start"]

    # If there's no source in the country then return 0
    if not found_country:
        return result

    # Set of all cities from populated places which are in given_country
    cities = set()
    if "populated_places" in knowledge_graph:
        pop_places = knowledge_graph["populated_places"]
        for place in pop_places:
            if pop_places[place][0]["metadata"]["country"] == given_country:
                cities.add(pop_places[place][0]["value"])

    dist = list()  # all distances between cities and country
    if "city" in knowledge_graph:
        kg_cities = knowledge_graph["city"]
        for city in kg_cities:
            if city in cities:
                for cp in kg_cities[city]:
                    if "origin" in cp:
                        if cp["origin"]["segment"] in country_start:
                            city_end = math.fabs(country_start[cp["origin"]["segment"]] - cp["context"]["end"])
                            if city_end <= 3:
                                dist.append(math.fabs(city_end))
    if dist:
        result = len(dist)
    # return 1 if result != 0 else 0  ---- do this if you want a binary feature
    return result


def country_next_to_state(knowledge_graph, given_country, state_to_country_dict):
    """ Distance between the country token and the state token.
    If the country token doesn't exist, result is 0. Else, if it exists,
    it is the min distance between end of all states and start of country"""
    result = 0
    found_country = False
    country_start = dict()  # dict of segment: start token val

    country_prov = knowledge_graph["country"][given_country]
    for prov in country_prov:
        if "origin" in prov:
            found_country = True
            country_start[prov["origin"]["segment"]] = prov["context"]["start"]

    # If there's no source in the country then return 0
    if not found_country:
        return result

    dist = list()  # all distances between states and country
    if "state" in knowledge_graph:
        kg_states = knowledge_graph["state"]
        for state in kg_states:
            if state in state_to_country_dict:
                if given_country in state_to_country_dict[state]:
                    for sp in kg_states[state]:
                        if "origin" in sp:
                            if sp["origin"]["segment"] in country_start:
                                state_end = math.fabs(country_start[sp["origin"]["segment"]] - sp["context"]["end"])
                                if state_end <= 3:
                                    dist.append(math.fabs(state_end))

    if dist:
        result = len(dist)
    # return 1 if result != 0 else 0 ---- do this if you want a binary feature
    return result


def number_of_explicit_mentions(knowledge_graph, given_country):
    count = 0
    country_prov = knowledge_graph["country"][given_country]
    for prov in country_prov:
        if "origin" in prov:
            count += 1

    return count


def num_of_explicit_mentions_in_content_strict(knowledge_graph, given_country):
    count = 0
    country_prov = knowledge_graph["country"][given_country]
    for prov in country_prov:
        if "origin" in prov:
            if prov["origin"]["segment"] == "content_strict":
                count += 1

    return count


def num_of_explicit_mentions_in_url(knowledge_graph, given_country):
    count = 0
    country_prov = knowledge_graph["country"][given_country]
    for prov in country_prov:
        if "origin" in prov:
            if prov["origin"]["segment"] == "url":
                count += 1

    return count


def number_of_implicit_mentions(knowledge_graph, given_country):
    count = 0
    country_prov = knowledge_graph["country"][given_country]
    for prov in country_prov:
        if "origin" not in prov:
            count += 1

    return count


def bool_if_colon_next_to_any_city(knowledge_graph, given_country):
    colon = ":"
    count = 0
    # Set of all cities from populated places which are in given_country
    cities = set()
    if "populated_places" in knowledge_graph:
        pop_places = knowledge_graph["populated_places"]
        for place in pop_places:
            if pop_places[place][0]["metadata"]["country"] == given_country:
                cities.add(pop_places[place][0]["value"])

    dist = list()  # all distances between cities and country
    if "city" in knowledge_graph:
        kg_cities = knowledge_graph["city"]
        for city in kg_cities:
            if city in cities:
                for cp in kg_cities[city]:
                    if "context" in cp:
                        if "tokens_left" in cp["context"]:
                            if colon in cp["context"]["tokens_left"][-2:]:
                                count += 1

    return count

def highest_population_city_is_in_country(knowledge_graph, given_country):
    count = 0
    high_pop = 0
    high_pop_country = ""
    if "populated_places" in knowledge_graph:
        pop_places = knowledge_graph["populated_places"]
        for place in pop_places:
            if "population" in pop_places[place][0]["metadata"]:
                if pop_places[place][0]["metadata"]["population"] >= high_pop:
                    high_pop_country = pop_places[place][0]["metadata"]["country"]
    if given_country == high_pop_country:
        count = 1
    return count

def calc_country_feature(knowledge_graph, state_to_country_dict):
    """ Returns a list of dicts which has val: coutry and feature_vector: vector"""
    vectors = list()
    if "country" in knowledge_graph:
        countries = knowledge_graph["country"]
        for country in countries:
            vector = dict()
            vector["metadata"] = dict()
            vector["metadata"]["country"] = country
            feature_vector = list()
            # calc all features and append
            feature_vector.append(country_next_to_city(knowledge_graph, country))
            feature_vector.append(number_of_explicit_mentions(knowledge_graph, country))
            feature_vector.append(country_next_to_state(knowledge_graph, country, state_to_country_dict))
            feature_vector.append(num_of_explicit_mentions_in_content_strict(knowledge_graph, country))
            feature_vector.append(num_of_explicit_mentions_in_url(knowledge_graph, country))
            feature_vector.append(number_of_implicit_mentions(knowledge_graph, country))
            feature_vector.append(bool_if_colon_next_to_any_city(knowledge_graph, country))
            feature_vector.append(highest_population_city_is_in_country(knowledge_graph, country))
            ### Add features here
            vector["value"] = json.dumps(feature_vector)
            vectors.append(vector)
    
    return vectors
