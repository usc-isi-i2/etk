import json
import math


def country_next_to_city(knowledge_graph, given_country):
	""" Distance between the country token and the city token. 
	If the country token doesn't exist, result is 0. Else, if it exists, 
	it is the min distance between end of all cities and start of country"""
	result = 0
	found_country = False
	country_start = dict() # dict of segment: start token val
	
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

	dist = list() # all distances between cities and country
	if "city" in knowledge_graph:
		kg_cities = knowledge_graph["city"]
		for city in kg_cities:
			if city in cities:
				for cp in kg_cities[city]:
					if "origin" in cp:
						if cp["origin"]["segment"] in country_start:
							city_end = country_start[cp["origin"]["segment"]] - cp["context"]["end"]
							dist.append(math.fabs(city_end))
	if dist:
		result = min(dist)
	return result


def country_next_to_state(knowledge_graph, given_country, state_to_country_dict):
	""" Distance between the country token and the state token. 
	If the country token doesn't exist, result is 0. Else, if it exists, 
	it is the min distance between end of all states and start of country"""
	result = 0
	found_country = False
	country_start = dict() # dict of segment: start token val
	
	country_prov = knowledge_graph["country"][given_country]
	for prov in country_prov:
		if "origin" in prov:
			found_country = True
			country_start[prov["origin"]["segment"]] = prov["context"]["start"]

	# If there's no source in the country then return 0
	if not found_country:
		return result

	dist = list() # all distances between states and country
	if "state" in knowledge_graph:
		kg_states = knowledge_graph["state"]
		for state in kg_states:
			if given_country in state_to_country_dict[state]:
				for sp in kg_states[state]:
					if "origin" in sp:
						if sp["origin"]["segment"] in country_start:
							state_end = country_start[sp["origin"]["segment"]] - sp["context"]["end"]
							dist.append(math.fabs(state_end))

	if dist:
		result = min(dist)
	return result


def number_of_explicit_mentions(knowledge_graph, given_country):
	count = 0
	country_prov = knowledge_graph["country"][given_country]
	for prov in country_prov:
		if "origin" in prov:
			count += 1

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
			### Add features here
			vector["value"] = json.dumps(feature_vector)
			vectors.append(vector)

	return vectors
