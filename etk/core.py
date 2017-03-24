# import all extractors
from data_extractors import *
from data_extractors import spacy_extractor
from data_extractors import landmark_extraction
from structured_extractors import ReadabilityExtractor, TokenizerExtractor
import json
import gzip
import re

_EXTRACTION_POLICY = 'extraction_policy'
_KEEP_EXISTING = 'keep_existing'
_REPLACE = 'replace'
_ERROR_HANDLING = 'error_handling'
_IGNORE_EXTRACTION = 'ignore_extraction'
_IGNORE_DOCUMENT = 'ignore_document'
_RAISE_ERROR = 'raise_error'
_CITY = 'city'
_CONTENT_EXTRACTION = 'content_extraction'
_RAW_CONTENT = 'raw_content'
_INPUT_PATH = 'input_path'
_READABILITY = 'readability'
_LANDMARK = 'landmark'
_TITLE = 'title'
_STRICT = 'strict'
_FIELD_NAME = 'field_name'
_CONTENT_STRICT = 'content_strict'
_CONTENT_RELAXED = 'content_relaxed'
_YES = 'yes'
_NO = 'no'
_RECALL_PRIORITY = 'recall_priority'
_INFERLINK_EXTRACTIONS = 'inferlink_extractions'
_LANDMARK_THRESHOLD = 'landmark_threshold'
_LANDMARK_RULES = 'landmark_rules'
_URL = 'url'


class Core(object):

    def __init__(self, extraction_config=None):
        if extraction_config:
            self.extraction_config = extraction_config
        self.html_title_regex = r'<title>(.*)?</title>'
        self.dictionaries_path = 'resources/dictionaries'
        self.tries = dict()
        self.global_extraction_policy = None
        self.global_error_handling = None

    """ Define all API methods """

    def process(self, doc):
        if self.extraction_config:
            if _EXTRACTION_POLICY in self.extraction_config:
                self.global_extraction_policy = self.extraction_config[_EXTRACTION_POLICY]
            if _ERROR_HANDLING in self.extraction_config:
                self.global_error_handling = self.extraction_config[_ERROR_HANDLING]

            """Handle content extraction first aka Phase 1"""
            if _CONTENT_EXTRACTION in self.extraction_config:
                ce_config = self.extraction_config[_CONTENT_EXTRACTION]
                html_field = ce_config[_INPUT_PATH] if _INPUT_PATH in ce_config else _RAW_CONTENT
                if html_field not in doc:
                    raise KeyError('{} not found in doc'.format(ce_config[_INPUT_PATH]))
                for extractor in ce_config.keys():
                    if extractor == _READABILITY:
                        re_exractors = ce_config[extractor]
                        if isinstance(re_exractors, dict):
                            re_exractors = [re_exractors]
                        for re_extractor in re_exractors:
                            doc = self.run_readability(doc, html_field, re_extractor)
                    elif extractor == _TITLE:
                        doc = self.run_title(doc, html_field, ce_config[extractor])
                    elif extractor == _LANDMARK:
                        doc = self.run_landmark(doc, html_field, ce_config[extractor])
        return doc

    def run_landmark(self, doc, html_field, landmark_config):
        field_name = landmark_config[_FIELD_NAME] if _FIELD_NAME in landmark_config else _INFERLINK_EXTRACTIONS
        ep = self.determine_extraction_policy(landmark_config)
        extraction_rules = None
        if _LANDMARK_RULES in landmark_config:
            extraction_rules = landmark_config[_LANDMARK_RULES]
        if not extraction_rules:
            raise ValueError('Please submit valid landmark extraction rules')
        if _LANDMARK_THRESHOLD in landmark_config:
            pct = landmark_config[_LANDMARK_THRESHOLD]
            if not 0.0 <= pct <= 1.0:
                raise ValueError('landmark threshold should be a float between {} and {}'.format(0.0, 1.0))
        else:
            pct = 0.5
        if field_name not in doc or (field_name in doc and ep == _REPLACE):
            ifl_extractions = Core.extract_landmark(doc[html_field], doc[_URL], extraction_rules, pct)
            if ifl_extractions:
                doc[field_name] = ifl_extractions
        return doc

    def run_title(self, doc, html_field, title_config):
        field_name = title_config[_FIELD_NAME] if _FIELD_NAME in title_config else _TITLE
        ep = self.determine_extraction_policy(title_config)
        if field_name not in doc or (field_name in doc and ep == _REPLACE):
            doc[field_name] = self.extract_title(doc[html_field])
        return doc

    def run_readability(self, doc, html_field, re_config):
        recall_priority = False
        field_name = None
        html = doc[html_field]
        if _STRICT in re_config:
            recall_priority = False if re_config[_STRICT] == _YES else True
            field_name = _CONTENT_RELAXED if recall_priority else _CONTENT_STRICT
        options = {_RECALL_PRIORITY: recall_priority}

        if _FIELD_NAME in re_config:
            field_name = re_config[_FIELD_NAME]
        ep = self.determine_extraction_policy(re_config)

        if field_name not in doc or (field_name in doc and ep == _REPLACE):
            doc[field_name] = self.extract_readability(html, options)

        return doc


    def determine_extraction_policy(self, config):
        ep = None
        if _EXTRACTION_POLICY in config:
            ep = _EXTRACTION_POLICY
        elif self.global_extraction_policy:
            ep = self.global_extraction_policy
        if ep and (ep != _KEEP_EXISTING or ep != _REPLACE):
             raise ValueError('extraction_policy can either be {} or {}'.format(_KEEP_EXISTING, _REPLACE))
        if not ep:
            ep = _REPLACE  # By default run the extraction again
        return ep

    def load_trie(self, file_name):
        # values = json.load(codecs.open(file_name, 'r', 'utf-8'))
        values = json.load(gzip.open(file_name), 'utf-8')
        trie = populate_trie(map(lambda x: x.lower(), values))
        return trie

    def load_dictionary(self, field_name, dict_name):
        if field_name not in self.tries:
            self.tries[field_name] = self.load_trie(self.dictionaries_path + "/" + dict_name)

    # def load_dictionaries(self, paths=paths):
    #     for key, value in paths.iteritems():
    #         self.tries[key] = self.load_trie(value)

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

    @staticmethod
    def extract_landmark(html, url, extraction_rules, threshold=0.5):
        return landmark_extraction.landmark_extractor(html, url, extraction_rules, threshold)

