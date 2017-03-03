# import all extractors
from data_extractors import *
import json
import codecs
import gzip
import os


class Core(object):
	""" Define all API methods """

	path_to_dig_dict = os.path.dirname(os.path.abspath(__file__)) + "/dictionaries/"

	paths = {
	"cities": path_to_dig_dict + "curated_cities.json.gz",
	"haircolor": path_to_dig_dict + "haircolors-customized.json.gz",
    "eyecolor": path_to_dig_dict + "eyecolors-customized.json.gz",
    "ethnicities": path_to_dig_dict + "ethnicities.json.gz",
    "names": path_to_dig_dict + "female-names-master.json.gz"
	}

	tries = dict()

	def load_trie(self, file_name):
		# values = json.load(codecs.open(file_name, 'r', 'utf-8'))
		values = json.load(gzip.open(file_name), 'utf-8')
		trie = populate_trie(map(lambda x: x.lower(), values))
		return trie

	def load_dictionaries(self, paths = paths):
		for key, value in paths.iteritems():
			self.tries[key] = self.load_trie(value)

	def extract_using_dictionary(self, tokens, name, 
									pre_process = lambda x: x.lower(), 
									pre_filter = lambda x: x, 
									post_filter = lambda x: isinstance(x, basestring),
									ngrams = 1,
									joiner = ' '):
		""" Takes in tokens as input along with the dict name"""

		if name in self.tries:
			return extract_using_dictionary(tokens, pre_process = pre_process, 
												trie=self.tries[name], 
												pre_filter = pre_filter,
												post_filter = post_filter, 
												ngrams = ngrams, 
												joiner = joiner)
		else:
			print "wrong dict"
			return []

	def extract_address(self, document):
		""" 
		Takes text document as input. 
		
		Note:
		1. Add keyword list as a user parameter
		2. Add documentation
		3. Add unit tests
		"""

		return extract_address(document)

	"""
	def extract_age(self,doc):
		
		Extract Age(s) from document	
		
		Args:
			doc (str): Document from which age needs to be extracted.

		Returns <>
		

		#return	extract_age(doc)	 
	"""




