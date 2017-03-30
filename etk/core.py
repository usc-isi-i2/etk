# import all extractors
from spacy_extractors import *
from data_extractors import *
from data_extractors import spacy_extractor
from data_extractors import landmark_extraction
from structured_extractors import ReadabilityExtractor, TokenizerExtractor
import json
import gzip
import os
import re
import spacy

class Core(object):
    """ Define all API methods """

    # html_title_regex = r'<title>[\r|\n|\t]*(.*)[\r|\n|\t]*</title>'
    html_title_regex = r'<title>(.*)?</title>'

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

    def load_dictionaries(self, paths=paths):
        for key, value in paths.iteritems():
            self.tries[key] = self.load_trie(value)

    def extract_using_dictionary(self, tokens, name, pre_process=lambda x: x.lower(),
                                 pre_filter=lambda x: x,
                                 post_filter=lambda x: isinstance(x, basestring),
                                 ngrams=1,
                                 joiner=' '):
        """ Takes in tokens as input along with the dict name"""

        if name in self.tries:
            return extract_using_dictionary(tokens, pre_process=pre_process,
                                            trie=self.tries[name],
                                            pre_filter=pre_filter,
                                            post_filter=post_filter,
                                            ngrams=ngrams,
                                            joiner=joiner)
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

    def extract_readability(self, document, options={}):
        e = ReadabilityExtractor()
        return e.extract(document, options)

    def extract_title(self, html_content, options={}):
        matches = re.search(self.html_title_regex, html_content, re.IGNORECASE | re.S)
        title = None
        if matches:
            title = matches.group(1)
            title = title.replace('\r', '')
            title = title.replace('\n', '')
            title = title.replace('\t', '')
        if not title:
            title = ''
        return {'text': title}


    def extract_crftokens(self, text, options={}):
        t = TokenizerExtractor(recognize_linebreaks=True, create_structured_tokens=True).set_metadata(
            {'extractor': 'crf_tokenizer'})
        return t.extract(text)

    def extract_tokens_from_crf(self, crf_tokens):
        return [tk['value'] for tk in crf_tokens]

    def extract_table(self, html_doc):
        return table_extract(html_doc)

    def extract_age(self, doc):
        '''
        Args:
            doc (str): Document

        Returns:
            List of age extractions with context and value

        Examples:
            >>> tk.extract_age('32 years old')
            [{'context': {'field': 'text', 'end': 11, 'start': 0}, 'value': '32'}]
        '''

        return age_extract(doc)

    def extract_weight(self, doc):
        '''
        Args:
            doc (str): Document

        Returns:
            List of weight extractions with context and value

        Examples:
            >>> tk.extract_age('Weight 10kg')
            [{'context': {'field': 'text', 'end': 7, 'start': 11}, 'value': {'unit': 'kilogram', 'value': 10}}]
        '''

        return weight_extract(doc)

    def extract_height(self, doc):
        '''
        Args:
            doc (str): Document

        Returns:
            List of height extractions with context and value

        Examples:
            >>> tk.extract_age('Height 5'3\"')
            [{'context': {'field': 'text', 'end': 7, 'start': 12}, 'value': {'unit': 'foot/inch', 'value': '5\'3"'}}]
        '''

        return height_extract(doc)

    def extract_stock_tickers(self, doc):
        return extract_stock_tickers(doc)

    def extract_spacy(self, doc):
        return spacy_extractor.spacy_extract(doc)

    def extract_landmark(self, doc, extraction_rules=None):
        if extraction_rules:
            return landmark_extraction.landmark_extractor(doc, extraction_rules)
        return doc

    def extract_price(self, doc):
    	digpe = DIGPriceExtractor()
    	price = digpe.extract(doc)
    	if price['price'] or price['price_per_hour']:
    		return price
    	return None

    # spaCy
    def load_matchers(self):
        nlp = spacy.load('en')
        self.nlp = nlp
        self.spacy_tokenizer = self.nlp.tokenizer

        matchers = {}

        # Load date_extractor matcher
        matchers['date'] = load_date_matcher(nlp)

        self.matchers = matchers

    def extract_date_spacy(self, doc):

        # Do the extraction
        result = extract_date_spacy(self.nlp, self.matchers['date'], self, doc)

        # Replace tokenizer
        self.nlp.tokenizer = self.spacy_tokenizer

        return result