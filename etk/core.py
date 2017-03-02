# import all extractors
from data_extractors import *
import json
import codecs


class Core(object):
	""" Define all API methods """

	# Set the path to dig-dictionaries repo here
	path_to_dig_dict = "/home/vinay/Documents/Study/ISI/dig-dictionaries/"

	paths = {
	"cities": path_to_dig_dict + "geonames-populated-places/curated_cities.json",
	"haircolor": path_to_dig_dict + "haircolor/haircolors-customized.json",
    "eyecolor": path_to_dig_dict + "eyecolor/eyecolors-customized.json",
    "ethnicities": path_to_dig_dict + "ethnicities/ethnicities.json",
    "names": path_to_dig_dict + "person-names/female-names-master.json"
	}

	tries = dict()

	def load_trie(self, file_name):
		values = json.load(codecs.open(file_name, 'r', 'utf-8'))
		trie = populate_trie(map(lambda x: x.lower(), values))
		return trie

	def load_dictionaries(self, paths = paths):
		for key, value in paths.iteritems():
			self.tries[key] = self.load_trie(value)

	def extract_using_dictionary(self, tokens, name):
		""" Takes in tokens as input along with the dict name"""

		if name in self.tries:
			return extract_using_dictionary(tokens, pre_process = lambda x: x.lower(), trie=self.tries[name])
		else:
			print "wrong dict"
			return []






