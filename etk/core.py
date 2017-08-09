import sys

stdout = sys.stdout
reload(sys)
sys.setdefaultencoding('utf-8')
sys.stdout = stdout
# import all extractors
from spacy_extractors import age_extractor as spacy_age_extractor
from spacy_extractors import social_media_extractor as spacy_social_media_extractor
from spacy_extractors import date_extractor as spacy_date_extractor
from spacy_extractors import address_extractor as spacy_address_extractor
from spacy_extractors import customized_extractor as custom_spacy_extractor
from data_extractors import landmark_extraction
from data_extractors import dictionary_extractor
from data_extractors import regex_extractor
from data_extractors import height_extractor
from data_extractors import weight_extractor
from data_extractors import address_extractor
from data_extractors import age_extractor
from data_extractors import table_extractor
from data_extractors import url_country_extractor
from data_extractors import geonames_extractor
from data_extractors.digPhoneExtractor import phone_extractor
from data_extractors.digEmailExtractor import email_extractor
from data_extractors.digPriceExtractor import price_extractor
from data_extractors.digReviewIDExtractor import review_id_extractor
from data_extractors import date_parser
from classifiers import country_classifier
from structured_extractors import ReadabilityExtractor, TokenizerExtractor, FaithfulTokenizerExtractor
import json
import gzip
import re
import spacy
import codecs
from jsonpath_rw import parse
import time
import collections
import numbers
from tldextract import tldextract
import pickle
import copy
from collections import OrderedDict
import sys

_KNOWLEDGE_GRAPH = "knowledge_graph"
_EXTRACTION_POLICY = 'extraction_policy'
_KEEP_EXISTING = 'keep_existing'
_REPLACE = 'replace'
_ERROR_HANDLING = 'error_handling'
_IGNORE_EXTRACTION = 'ignore_extraction'
_IGNORE_DOCUMENT = 'ignore_document'
_RAISE_ERROR = 'raise_error'
_CITY_NAME = 'city_name'
_STATE = 'state'
_COUNTRY = 'country'
_CONTENT_EXTRACTION = 'content_extraction'
_SPACY_EXTRACTION = 'spacy_extraction'
_RAW_CONTENT = 'raw_content'
_INPUT_PATH = 'input_path'
_READABILITY = 'readability'
_LANDMARK = 'landmark'
_TITLE = 'title'
_DESCRIPTION = "description"
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
_AGE = 'age'
_POSTING_DATE = 'posting_date'
_SOCIAL_MEDIA = 'social_media'
_ADDRESS = 'address'
_RESOURCES = 'resources'
_SPACY_FIELD_RULES = "spacy_field_rules"
_DATA_EXTRACTION = 'data_extraction'
_FIELDS = 'fields'
_EXTRACTORS = 'extractors'
_TOKENS = 'tokens'
_TOKENS_ORIGINAL_CASE = "tokens_original_case"
_SIMPLE_TOKENS = 'simple_tokens'
_SIMPLE_TOKENS_ORIGINAL_CASE = 'simple_tokens_original_case'
_TEXT = 'text'
_DICTIONARY = 'dictionary'
_PICKLES = 'pickle'
_NGRAMS = 'ngrams'
_JOINER = 'joiner'
_PRE_FILTER = 'pre_filter'
_POST_FILTER = 'post_filter'
_PRE_PROCESS = "pre_process"
_TABLE = "table"
_STOP_WORDS = "stop_words"
_GEONAMES = "geonames"
_STATE_TO_COUNTRY = "state_to_country"
_STATE_TO_CODES_LOWER = "state_to_codes_lower"
_POPULATED_PLACES = "populated_places"
_POPULATED_CITIES = "populated_cities"
_CASE_SENSITIVE = 'case_sensitive'

_EXTRACT_USING_DICTIONARY = "extract_using_dictionary"
_EXTRACT_USING_REGEX = "extract_using_regex"
_EXTRACT_FROM_LANDMARK = "extract_from_landmark"
_EXTRACT_PHONE = "extract_phone"
_EXTRACT_EMAIL = "extract_email"
_EXTRACT_PRICE = "extract_price"
_EXTRACT_HEIGHT = "extract_height"
_EXTRACT_WEIGHT = "extract_weight"
_EXTRACT_ADDRESS = "extract_address"
_EXTRACT_AGE = "extract_age"

_CONFIG = "config"
_DICTIONARIES = "dictionaries"
_INFERLINK = "inferlink"
_HTML = "html"

_SEGMENT_TITLE = "title"
_SEGMENT_INFERLINK_DESC = "inferlink_description"
_SEGMENT_OTHER = "other_segment"

_METHOD_INFERLINK = "inferlink"

_SOURCE_TYPE = "source_type"
_OBFUSCATION = "obfuscation"

_INCLUDE_CONTEXT = "include_context"
_KG_ENHANCEMENT = "kg_enhancement"
_DOCUMENT_ID = "document_id"
_TLD = 'tld'
_FEATURE_COMPUTATION = "feature_computation"


class Core(object):
    def __init__(self, extraction_config=None, debug=False, load_spacy=False):
        self.extraction_config = extraction_config
        self.debug = debug
        self.html_title_regex = r'<title>(.*?)</title>'
        self.tries = dict()
        self.pickles = dict()
        self.jobjs = dict()
        self.global_extraction_policy = None
        self.global_error_handling = _RAISE_ERROR
        # to make sure we do not parse json_paths more times than needed, we define the following 2 properties
        self.content_extraction_path = None
        self.data_extraction_path = dict()
        if load_spacy:
            self.prep_spacy()
        else:
            self.nlp = None
        self.country_code_dict = None
        self.matchers = dict()
        self.geonames_dict = None
        self.state_to_country_dict = None
        self.state_to_codes_lower_dict = None
        self.populated_cities = None

    """ Define all API methods """

    def process(self, doc, create_knowledge_graph=False):
        try:
            # print 'Now Processing url: {}, doc_id: {}'.format(doc['url'], doc['doc_id'])
            if self.extraction_config:
                doc_id = None
                if _DOCUMENT_ID in self.extraction_config:
                    doc_id_field = self.extraction_config[_DOCUMENT_ID]
                    if doc_id_field in doc:
                        doc_id = doc[doc_id_field]
                        doc[_DOCUMENT_ID] = doc_id
                    else:
                        raise KeyError('{} not found in the input document'.format(doc_id_field))
                if _EXTRACTION_POLICY in self.extraction_config:
                    self.global_extraction_policy = self.extraction_config[_EXTRACTION_POLICY]
                    error_handling = self.extraction_config[
                        _ERROR_HANDLING] if _ERROR_HANDLING in self.extraction_config else _RAISE_ERROR
                    if error_handling != _RAISE_ERROR or error_handling != _IGNORE_DOCUMENT:
                        print 'WARN: error handling in extraction config can either be \"{}\" or \"{}\".' \
                              ' By default its value has been set to \"{}\"'.format(
                            _RAISE_ERROR, _IGNORE_DOCUMENT, _RAISE_ERROR)
                        error_handling = _RAISE_ERROR
                    self.global_error_handling = error_handling

                """Handle content extraction first aka Phase 1"""
                if _CONTENT_EXTRACTION in self.extraction_config:
                    if _CONTENT_EXTRACTION not in doc:
                        doc[_CONTENT_EXTRACTION] = dict()
                    ce_config = self.extraction_config[_CONTENT_EXTRACTION]
                    html_path = ce_config[_INPUT_PATH] if _INPUT_PATH in ce_config else None
                    if not html_path:
                        raise KeyError('{} not found in extraction_config'.format(_INPUT_PATH))

                    if not self.content_extraction_path:
                        start_time = time.time()
                        self.content_extraction_path = parse(html_path)
                        time_taken = time.time() - start_time
                        if self.debug:
                            print 'time taken to process parse %s' % time_taken
                    start_time = time.time()
                    matches = self.content_extraction_path.find(doc)
                    time_taken = time.time() - start_time
                    if self.debug:
                        print 'time taken to process matches %s' % time_taken
                    extractors = ce_config[_EXTRACTORS]
                    for index in range(len(matches)):
                        for extractor in extractors.keys():
                            if extractor == _READABILITY:
                                # TODO REMOVE THIS HACK
                                if len(matches[index].value) < 700000 and 'amigobulls.com' not in doc['url'] and 'kulakowka.com' not in doc['url']:
                                    re_extractors = extractors[extractor]
                                    if isinstance(re_extractors, dict):
                                        re_extractors = [re_extractors]
                                    for re_extractor in re_extractors:
                                        doc[_CONTENT_EXTRACTION] = self.run_readability(doc[_CONTENT_EXTRACTION],
                                                                                        matches[index].value,
                                                                                        re_extractor)
                                else:
                                    print 'Large document not running READABILITY, doc_id: {}'.format(doc['doc_id'])
                            elif extractor == _TITLE:
                                doc[_CONTENT_EXTRACTION] = self.run_title(doc[_CONTENT_EXTRACTION],
                                                                          matches[index].value,
                                                                          extractors[extractor])
                            elif extractor == _LANDMARK:
                                doc[_CONTENT_EXTRACTION] = self.run_landmark(doc[_CONTENT_EXTRACTION],
                                                                             matches[index].value,
                                                                             extractors[extractor], doc[_URL])
                            elif extractor == _TABLE:
                                doc[_CONTENT_EXTRACTION] = self.run_table_extractor(doc[_CONTENT_EXTRACTION],
                                                                                    matches[index].value,
                                                                                    extractors[extractor])
                    # Add the url as segment as well
                    if _URL in doc and doc[_URL] and doc[_URL].strip() != '':
                        doc[_CONTENT_EXTRACTION][_URL] = dict()
                        doc[_CONTENT_EXTRACTION][_URL][_TEXT] = doc[_URL]
                        doc[_TLD] = self.extract_tld(doc[_URL])

                """Phase 2: The Data Extraction"""
                if _DATA_EXTRACTION in self.extraction_config:
                    de_configs = self.extraction_config[_DATA_EXTRACTION]
                    if isinstance(de_configs, dict):
                        de_configs = [de_configs]

                    for i in range(len(de_configs)):
                        de_config = de_configs[i]
                        input_paths = de_config[_INPUT_PATH] if _INPUT_PATH in de_config else None
                        if not input_paths:
                            raise KeyError('{} not found for data extraction in extraction_config'.format(_INPUT_PATH))

                        if not isinstance(input_paths, list):
                            input_paths = [input_paths]

                        for input_path in input_paths:
                            if _FIELDS in de_config:
                                if input_path not in self.data_extraction_path:
                                    self.data_extraction_path[input_path] = parse(input_path)
                                matches = self.data_extraction_path[input_path].find(doc)
                                for match in matches:
                                    # First rule of DATA Extraction club: Get tokens
                                    # Get the crf tokens
                                    if _TEXT in match.value:
                                        if _TOKENS_ORIGINAL_CASE not in match.value:
                                            match.value[_TOKENS_ORIGINAL_CASE] = self.extract_crftokens(
                                                match.value[_TEXT],
                                                lowercase=False)
                                        if _TOKENS not in match.value:
                                            match.value[_TOKENS] = self.crftokens_to_lower(
                                                match.value[_TOKENS_ORIGINAL_CASE])
                                        if _SIMPLE_TOKENS not in match.value:
                                            match.value[_SIMPLE_TOKENS] = self.extract_tokens_from_crf(
                                                match.value[_TOKENS])
                                        if _SIMPLE_TOKENS_ORIGINAL_CASE not in match.value:
                                            match.value[_SIMPLE_TOKENS_ORIGINAL_CASE] = self.extract_tokens_from_crf(
                                                match.value[_TOKENS_ORIGINAL_CASE])
                                            # if _TOKENS not in match.value:
                                            #     match.value[_TOKENS] = self.extract_crftokens(match.value[_TEXT])
                                            # if _SIMPLE_TOKENS not in match.value:
                                            #     match.value[_SIMPLE_TOKENS] = self.extract_tokens_from_crf(match.value[_TOKENS])
                                    fields = de_config[_FIELDS]
                                    for field in fields.keys():
                                        run_extractor = True
                                        full_path = str(match.full_path)
                                        segment = self.determine_segment(full_path)
                                        if field != '*':
                                            """
                                                Special case for inferlink extractions:
                                                For eg, We do not want to extract name from inferlink_posting-date #DUH
                                            """
                                            if _INFERLINK in full_path:
                                                if field not in full_path:
                                                    run_extractor = False
                                                if _DESCRIPTION in full_path or _TITLE in full_path:
                                                    run_extractor = True
                                            if run_extractor:
                                                if _EXTRACTORS in fields[field]:
                                                    extractors = fields[field][_EXTRACTORS]
                                                    for extractor in extractors.keys():
                                                        try:
                                                            foo = getattr(self, extractor)
                                                        except:
                                                            foo = None
                                                        if foo:
                                                            # score is 1.0 because every method thinks it is the best
                                                            score = 1.0
                                                            method = extractor
                                                            if _CONFIG not in extractors[extractor]:
                                                                extractors[extractor][_CONFIG] = dict()
                                                            extractors[extractor][_CONFIG][_FIELD_NAME] = field
                                                            ep = self.determine_extraction_policy(extractors[extractor])
                                                            if extractor == _EXTRACT_FROM_LANDMARK:
                                                                if _INFERLINK_EXTRACTIONS in full_path and field in full_path:
                                                                    method = _METHOD_INFERLINK
                                                                    if self.check_if_run_extraction(match.value, field,
                                                                                                    extractor,
                                                                                                    ep):

                                                                        results = foo(doc,
                                                                                      extractors[extractor][_CONFIG])
                                                                        if results:
                                                                            self.add_data_extraction_results(
                                                                                match.value,
                                                                                field,
                                                                                extractor,
                                                                                self.add_origin_info(
                                                                                    results,
                                                                                    method,
                                                                                    segment,
                                                                                    score,
                                                                                    doc_id))
                                                                            if create_knowledge_graph:
                                                                                self.create_knowledge_graph(doc, field,
                                                                                                            results)
                                                            else:
                                                                if self.check_if_run_extraction(match.value, field,
                                                                                                extractor,
                                                                                                ep):
                                                                    results = foo(match.value,
                                                                                  extractors[extractor][_CONFIG])
                                                                    if results:
                                                                        self.add_data_extraction_results(match.value,
                                                                                                         field,
                                                                                                         extractor,
                                                                                                         self.add_origin_info(
                                                                                                             results,
                                                                                                             method,
                                                                                                             segment,
                                                                                                             score,
                                                                                                             doc_id))
                                                                        if create_knowledge_graph:
                                                                            self.create_knowledge_graph(doc, field,
                                                                                                        results)
                                        else:  # extract whatever you can!
                                            if _EXTRACTORS in fields[field]:
                                                extractors = fields[field][_EXTRACTORS]
                                                for extractor in extractors.keys():
                                                    try:
                                                        foo = getattr(self, extractor)
                                                    except Exception as e:
                                                        foo = None
                                                    if foo:
                                                        # score is 1.0 because every method thinks it is the best
                                                        score = 1.0
                                                        method = extractor
                                                        if _CONFIG not in extractors[extractor]:
                                                            extractors[extractor][_CONFIG] = dict()
                                                        ep = self.determine_extraction_policy(extractors[extractor])
                                                        if extractor == _EXTRACT_FROM_LANDMARK:
                                                            if _INFERLINK_EXTRACTIONS in full_path and field in full_path:
                                                                method = _METHOD_INFERLINK
                                                                if self.check_if_run_extraction(match.value, field,
                                                                                                extractor,
                                                                                                ep):

                                                                    results = foo(doc, extractors[extractor][_CONFIG])
                                                                    if results:
                                                                        self.add_data_extraction_results(match.value,
                                                                                                         field,
                                                                                                         extractor,
                                                                                                         self.add_origin_info(
                                                                                                             results,
                                                                                                             method,
                                                                                                             segment,
                                                                                                             score,
                                                                                                             doc_id))
                                                                        if create_knowledge_graph:
                                                                            self.create_knowledge_graph(doc, field,
                                                                                                        results)
                                                        else:
                                                            results = foo(match.value,
                                                                          extractors[extractor][_CONFIG])
                                                            if results:
                                                                for f, res in results.items():
                                                                    # extractors[extractor][_CONFIG][_FIELD_NAME] = f
                                                                    self.add_data_extraction_results(match.value, f,
                                                                                                     extractor,
                                                                                                     self.add_origin_info(
                                                                                                         res,
                                                                                                         method,
                                                                                                         segment,
                                                                                                         score, doc_id))
                                                                    if create_knowledge_graph:
                                                                        self.create_knowledge_graph(doc, f, res)
                                                    else:
                                                        print('method {} not found!'.format(extractor))

                """Optional Phase 3: Knowledge Graph Enhancement"""
                if _KG_ENHANCEMENT in self.extraction_config:
                    kg_configs = self.extraction_config[_KG_ENHANCEMENT]
                    if isinstance(kg_configs, dict):
                        kg_configs = [kg_configs]

                    for i in range(len(kg_configs)):
                        kg_config = kg_configs[i]
                        input_paths = kg_config[_INPUT_PATH] if _INPUT_PATH in kg_config else None
                        if not input_paths:
                            raise KeyError(
                                '{} not found for knowledge graph enhancement in extraction_config'.format(_INPUT_PATH))

                        if not isinstance(input_paths, list):
                            input_paths = [input_paths]

                        for input_path in input_paths:
                            if _FIELDS in kg_config:
                                if input_path not in self.data_extraction_path:
                                    self.data_extraction_path[input_path] = parse(input_path)
                                matches = self.data_extraction_path[input_path].find(doc)
                                for match in matches:
                                    fields = kg_config[_FIELDS]
                                    try:
                                        sorted_fields = self.sort_dictionary_by_fields(fields)
                                    except:
                                        raise ValueError('Please ensure there is a priority added to every field in '
                                                         'knowledge_graph  enhancement and the priority is an int')
                                    for i in range(0, len(sorted_fields)):
                                        field = sorted_fields[i][0]
                                        if _EXTRACTORS in fields[field]:
                                            extractors = fields[field][_EXTRACTORS]
                                            for extractor in extractors.keys():
                                                try:
                                                    foo = getattr(self, extractor)
                                                except:
                                                    foo = None
                                                if foo:
                                                    if _CONFIG not in extractors[extractor]:
                                                        extractors[extractor][_CONFIG] = dict()
                                                    extractors[extractor][_CONFIG][_FIELD_NAME] = field
                                                    results = foo(match.value, extractors[extractor][_CONFIG])
                                                    if results:
                                                        # doc[_KNOWLEDGE_GRA][field] = results
                                                        self.create_knowledge_graph(doc, field, results)

                """Optional Phase 4: feature computation"""
                if _FEATURE_COMPUTATION in self.extraction_config:
                    kg_configs = self.extraction_config[_FEATURE_COMPUTATION]
                    if isinstance(kg_configs, dict):
                        kg_configs = [kg_configs]

                    for i in range(len(kg_configs)):
                        kg_config = kg_configs[i]
                        input_paths = kg_config[_INPUT_PATH] if _INPUT_PATH in kg_config else None
                        if not input_paths:
                            raise KeyError(
                                '{} not found for feature computation in extraction_config'.format(_INPUT_PATH))

                        if not isinstance(input_paths, list):
                            input_paths = [input_paths]

                        for input_path in input_paths:
                            if _FIELDS in kg_config:
                                if input_path not in self.data_extraction_path:
                                    self.data_extraction_path[input_path] = parse(input_path)
                                matches = self.data_extraction_path[input_path].find(doc)
                                for match in matches:
                                    fields = kg_config[_FIELDS]
                                    for field in fields.keys():
                                        if _EXTRACTORS in fields[field]:
                                            extractors = fields[field][_EXTRACTORS]
                                            for extractor in extractors.keys():
                                                try:
                                                    foo = getattr(self, extractor)
                                                except:
                                                    foo = None
                                                if foo:
                                                    if _CONFIG not in extractors[extractor]:
                                                        extractors[extractor][_CONFIG] = dict()
                                                    extractors[extractor][_CONFIG][_FIELD_NAME] = field
                                                    results = foo(match.value, extractors[extractor][_CONFIG])
                                                    if results:
                                                        # doc[_KNOWLEDGE_GRAPH][field] = results
                                                        self.create_knowledge_graph(doc, field, results)

                if _KNOWLEDGE_GRAPH in doc and doc[_KNOWLEDGE_GRAPH]:
                    doc[_KNOWLEDGE_GRAPH] = self.reformat_knowledge_graph(doc[_KNOWLEDGE_GRAPH])
                    """ Add title and description as fields in the knowledge graph as well"""
                    doc = Core.rearrange_description(doc)
                    doc = Core.rearrange_title(doc)
        except Exception as e:
            if self.global_error_handling == _RAISE_ERROR:
                raise e
            else:
                print e
                print 'Failed doc:', doc['doc_id']
                return None
        # print 'DONE url: {}, doc_id: {}'.format(doc['url'], doc['doc_id'])
        return doc

    @staticmethod
    def rearrange_description(doc):
        method = 'rearrange_description'
        description = None
        segment = ''
        if _CONTENT_EXTRACTION in doc:
            ce = doc[_CONTENT_EXTRACTION]
            if _INFERLINK_EXTRACTIONS in ce:
                if _CONTENT_RELAXED in ce:
                    description = ce[_CONTENT_RELAXED][_TEXT]
                    segment = _CONTENT_RELAXED
                elif _DESCRIPTION in ce[_INFERLINK_EXTRACTIONS]:
                    description = ce[_INFERLINK_EXTRACTIONS][_DESCRIPTION][_TEXT]
                    segment = _INFERLINK
            if not description or description.strip() == '':
                if _CONTENT_STRICT in ce:
                    description = ce[_CONTENT_STRICT][_TEXT]
                    segment = _CONTENT_STRICT

            if description and description != '':
                if _KNOWLEDGE_GRAPH not in doc:
                    doc[_KNOWLEDGE_GRAPH] = dict()
                doc[_KNOWLEDGE_GRAPH][_DESCRIPTION] = list()
                o = dict()
                o['value'] = description
                o['key'] = 'description'
                o['confidence'] = 1
                o['provenance'] = [Core.custom_provenance_object(method, segment, doc[_DOCUMENT_ID])]
                doc[_KNOWLEDGE_GRAPH][_DESCRIPTION].append(o)
        return doc

    @staticmethod
    def sort_dictionary_by_fields(dictionary):
        sorted_d = OrderedDict(sorted(dictionary.iteritems(), key=lambda x: x[1]['priority']))
        return sorted_d.items()

    @staticmethod
    def custom_provenance_object(method, segment, document_id):
        prov = dict()
        prov['method'] = method
        prov['source'] = dict()
        prov['source']['segment'] = segment
        prov['source'][_DOCUMENT_ID] = document_id
        return prov

    @staticmethod
    def rearrange_title(doc):
        method = 'rearrange_title'
        title = None
        segment = ''
        if _CONTENT_EXTRACTION in doc:
            ce = doc[_CONTENT_EXTRACTION]
            if _INFERLINK_EXTRACTIONS in ce:
                if _TITLE in ce[_INFERLINK_EXTRACTIONS]:
                    title = ce[_INFERLINK_EXTRACTIONS][_TITLE][_TEXT]
                    segment = _INFERLINK
            if not title or title.strip() == '':
                if _TITLE in ce:
                    title = ce[_TITLE][_TEXT]
                    segment = _HTML
            if not title or title.strip() == '':
                if _CONTENT_RELAXED in ce:
                    vals = ce[_CONTENT_RELAXED][_TEXT].split(' ')
                    new_vals = list()
                    for i in range(0, len(vals)):
                        if len(new_vals) == 10:
                            break
                        if vals[i].strip() != '':
                            new_vals.append(vals[i])
                    title = ' '.join(new_vals)
                    segment = _HTML

            if title and title != '':
                if _KNOWLEDGE_GRAPH not in doc:
                    doc[_KNOWLEDGE_GRAPH] = dict()
                doc[_KNOWLEDGE_GRAPH][_TITLE] = list()
                o = dict()
                o['value'] = title
                o['key'] = 'title'
                o['confidence'] = 1
                o['provenance'] = [Core.custom_provenance_object(method, segment, doc[_DOCUMENT_ID])]
                doc[_KNOWLEDGE_GRAPH][_TITLE].append(o)

        return doc

    @staticmethod
    def extract_tld(url):
        return tldextract.extract(url).domain + '.' + tldextract.extract(url).suffix

    @staticmethod
    def create_knowledge_graph(doc, field_name, extractions):
        if _KNOWLEDGE_GRAPH not in doc:
            doc[_KNOWLEDGE_GRAPH] = dict()

        if field_name not in doc[_KNOWLEDGE_GRAPH]:
            doc[_KNOWLEDGE_GRAPH][field_name] = dict()

        for extraction in extractions:
            key = extraction['value']
            if (isinstance(key, basestring) or isinstance(key, numbers.Number)) and field_name != _POSTING_DATE:
                # try except block because unicode characters will not be lowered
                try:
                    key = str(key).strip().lower()
                except:
                    pass
            if 'metadata' in extraction:
                sorted_metadata = Core.sort_dict(extraction['metadata'])
                for k, v in sorted_metadata.iteritems():
                    if isinstance(v, numbers.Number):
                        v = str(v)
                    # if v:
                    #     v = v.encode('utf-8')
                    if v and v.strip() != '':
                        # key += '-' + str(k) + ':' + str(v)
                        key = '{}-{}:{}'.format(key, k, v)

            if 'key' in extraction:
                key = extraction['key']

            # TODO FIX THIS HACK
            if len(key) > 32766:
                key = key[0:500]

            if key not in doc[_KNOWLEDGE_GRAPH][field_name]:
                doc[_KNOWLEDGE_GRAPH][field_name][key] = list()
            doc[_KNOWLEDGE_GRAPH][field_name][key].append(extraction)

        return doc

    @staticmethod
    def reformat_knowledge_graph(knowledge_graph):
        new_kg = dict()
        for semantic_type in knowledge_graph.keys():
            new_kg[semantic_type] = list()
            values = knowledge_graph[semantic_type]
            for key in values.keys():
                o = dict()
                o['key'] = key
                new_provenances, metadata, value = Core.rearrange_provenance(values[key])
                o['provenance'] = new_provenances
                if metadata:
                    o['qualifiers'] = metadata
                o['value'] = value
                # default confidence value, to be updated by later analysis
                o['confidence'] = 1
                new_kg[semantic_type].append(o)
        return new_kg

    @staticmethod
    def rearrange_provenance(old_provenances):
        new_provenances = list()
        metadata = None
        value = None
        for prov in old_provenances:
            new_prov = dict()
            method = None
            confidence = None

            if 'origin' in prov:
                origin = prov['origin']
                if 'obfuscation' in prov:
                    origin['extraction_metadata'] = dict()
                    origin['extraction_metadata']['obfuscation'] = prov['obfuscation']
                method = origin['method']
                confidence = origin['score']
                origin.pop('score', None)
                origin.pop('method', None)
                new_prov['source'] = origin

            if 'context' in prov:
                new_prov['source']['context'] = prov['context']
            if 'metadata' in prov and not metadata:
                metadata = prov['metadata']
            if method:
                new_prov["method"] = method
            if not value:
                value = prov['value']
            new_prov['extracted_value'] = value
            if confidence:
                new_prov['confidence'] = dict()
                new_prov['confidence']['extraction'] = confidence
            new_provenances.append(new_prov)
        return new_provenances, metadata, value

    @staticmethod
    def add_data_extraction_results(d, field_name, method_name, results):
        if _DATA_EXTRACTION not in d:
            d[_DATA_EXTRACTION] = dict()
        if field_name not in d[_DATA_EXTRACTION]:
            d[_DATA_EXTRACTION][field_name] = dict()
        if method_name not in d[_DATA_EXTRACTION][field_name]:
            d[_DATA_EXTRACTION][field_name][method_name] = dict()
        if isinstance(results, dict):
            results = [results]
        if 'results' not in d[_DATA_EXTRACTION][field_name][method_name]:
            d[_DATA_EXTRACTION][field_name][method_name]['results'] = results
        else:
            d[_DATA_EXTRACTION][field_name][method_name]['results'].extend(results)
        return d

    @staticmethod
    def check_if_run_extraction(d, field_name, method_name, extraction_policy):
        if _DATA_EXTRACTION not in d:
            return True
        if field_name not in d[_DATA_EXTRACTION]:
            return True
        if method_name not in d[_DATA_EXTRACTION][field_name]:
            return True
        if 'results' not in d[_DATA_EXTRACTION][field_name][method_name]:
            return True
        else:
            if extraction_policy == _REPLACE:
                return True
        return False

    @staticmethod
    def determine_segment(json_path):
        segment = _SEGMENT_OTHER
        if _SEGMENT_INFERLINK_DESC in json_path:
            segment = _SEGMENT_INFERLINK_DESC
        elif _INFERLINK in json_path and _SEGMENT_INFERLINK_DESC not in json_path:
            segment = _HTML
        elif _CONTENT_STRICT in json_path:
            segment = _CONTENT_STRICT
        elif _CONTENT_RELAXED in json_path:
            segment = _CONTENT_RELAXED
        elif _TITLE in json_path:
            segment = _TITLE
        elif _URL in json_path:
            segment = _URL
        return segment

    @staticmethod
    def add_origin_info(results, method, segment, score, doc_id=None):
        if results:
            for result in results:
                o = dict()
                o['segment'] = segment
                o['method'] = method
                o['score'] = score
                if doc_id:
                    o[_DOCUMENT_ID] = doc_id
                result['origin'] = o
        return results

    def run_landmark(self, content_extraction, html, landmark_config, url):
        field_name = landmark_config[_FIELD_NAME] if _FIELD_NAME in landmark_config else _INFERLINK_EXTRACTIONS
        ep = self.determine_extraction_policy(landmark_config)
        extraction_rules = self.consolidate_landmark_rules()
        if _LANDMARK_THRESHOLD in landmark_config:
            pct = landmark_config[_LANDMARK_THRESHOLD]
            if not 0.0 <= pct <= 1.0:
                raise ValueError('landmark threshold should be a float between {} and {}'.format(0.0, 1.0))
        else:
            pct = 0.5
        if field_name not in content_extraction or (field_name in content_extraction and ep == _REPLACE):
            start_time = time.time()
            ifl_extractions = Core.extract_landmark(html, url, extraction_rules, pct)

            if isinstance(ifl_extractions, list):
                # we have a rogue post type page, put it in its place
                field_name = 'inferlink_posts_special_text'
                content_extraction[field_name] = dict()
                content_extraction[field_name][_TEXT] = self.inferlink_posts_to_text(ifl_extractions)
            else:
                time_taken = time.time() - start_time
                if self.debug:
                    print 'time taken to process landmark %s' % time_taken
                if ifl_extractions and len(ifl_extractions.keys()) > 0:
                    content_extraction[field_name] = dict()
                    for key in ifl_extractions:
                        o = dict()
                        o[key] = dict()
                        o[key]['text'] = ifl_extractions[key]
                        content_extraction[field_name].update(o)
        return content_extraction

    @staticmethod
    def inferlink_posts_to_text(inferlink_posts):
        text = ''
        for inferlink_post in inferlink_posts:
            for k, v in inferlink_post.iteritems():
                if k == 'review_details':
                    for pair in v:
                        if 'key' in pair and 'value' in pair:
                            text += '{}_{}: {}\n'.format('review_details', pair['key'], pair['value'])
                else:
                    text += '{}: {}\n'.format(k, v)
        return text

    def consolidate_landmark_rules(self):
        rules = dict()
        if _RESOURCES in self.extraction_config:
            resources = self.extraction_config[_RESOURCES]
            if _LANDMARK in resources:
                landmark_rules_file_list = resources[_LANDMARK]
                for landmark_rules_file in landmark_rules_file_list:
                    rules.update(Core.load_json_file(landmark_rules_file))
                return rules
            else:
                raise KeyError('{}.{} not found in provided extraction config'.format(_RESOURCES, _LANDMARK))
        else:
            raise KeyError('{} not found in provided extraction config'.format(_RESOURCES))

    def get_dict_file_name_from_config(self, dict_name):
        if _RESOURCES in self.extraction_config:
            resources = self.extraction_config[_RESOURCES]
            if _DICTIONARIES in resources:
                if dict_name in resources[_DICTIONARIES]:
                    return resources[_DICTIONARIES][dict_name]
                else:
                    raise KeyError(
                        '{}.{}.{} not found in provided extraction config'.format(_RESOURCES, _DICTIONARIES, dict_name))
            else:
                raise KeyError('{}.{} not found in provided extraction config'.format(_RESOURCES, _DICTIONARIES))
        else:
            raise KeyError('{} not found in provided extraction config'.format(_RESOURCES))

    def get_pickle_file_name_from_config(self, pickle_name):
        if _RESOURCES in self.extraction_config:
            resources = self.extraction_config[_RESOURCES]
            if _PICKLES in resources:
                if pickle_name in resources[_PICKLES]:
                    return resources[_PICKLES][pickle_name]
                else:
                    raise KeyError(
                        '{}.{}.{} not found in provided extraction config'.format(_RESOURCES, _PICKLES, pickle_name))
            else:
                raise KeyError('{}.{} not found in provided extraction config'.format(_RESOURCES, _PICKLES))
        else:
            raise KeyError('{} not found in provided extraction config'.format(_RESOURCES))

    def get_spacy_field_rules_from_config(self, field_name):
        if _RESOURCES in self.extraction_config:
            resources = self.extraction_config[_RESOURCES]
            if _SPACY_FIELD_RULES in resources:
                if field_name in resources[_SPACY_FIELD_RULES]:
                    return resources[_SPACY_FIELD_RULES][field_name]
                else:
                    raise KeyError(
                        '{}.{}.{} not found in provided extraction config'.format(_RESOURCES, _SPACY_FIELD_RULES,
                                                                                  field_name))
            else:
                raise KeyError('{}.{} not found in provided extraction config'.format(_RESOURCES, _SPACY_FIELD_RULES))
        else:
            raise KeyError('{} not found in provided extraction config'.format(_RESOURCES))

    def run_title(self, content_extraction, html, title_config):
        field_name = title_config[_FIELD_NAME] if _FIELD_NAME in title_config else _TITLE
        ep = self.determine_extraction_policy(title_config)
        if field_name not in content_extraction or (field_name in content_extraction and ep == _REPLACE):
            start_time = time.time()
            extracted_title = self.extract_title(html)
            if extracted_title:
                content_extraction[field_name] = extracted_title
            time_taken = time.time() - start_time
            if self.debug:
                print 'time taken to process title %s' % time_taken
        return content_extraction

    def run_table_extractor(self, content_extraction, html, table_config):
        field_name = table_config[_FIELD_NAME] if _FIELD_NAME in table_config else _TABLE
        ep = self.determine_extraction_policy(table_config)
        if field_name not in content_extraction or (field_name in content_extraction and ep == _REPLACE):
            start_time = time.time()
            tables = self.extract_table(html, table_config)
            if tables is not None:
                content_extraction[field_name] = tables
            time_taken = time.time() - start_time
            if self.debug:
                print 'time taken to process table %s' % time_taken
        return content_extraction

    def run_readability(self, content_extraction, html, re_extractor):
        recall_priority = False
        field_name = None
        if _STRICT in re_extractor:
            recall_priority = False if re_extractor[_STRICT] == _YES else True
            field_name = _CONTENT_RELAXED if recall_priority else _CONTENT_STRICT
        options = {_RECALL_PRIORITY: recall_priority}

        if _FIELD_NAME in re_extractor:
            field_name = re_extractor[_FIELD_NAME]
        ep = self.determine_extraction_policy(re_extractor)
        start_time = time.time()
        readability_text = self.extract_readability(html, options)
        time_taken = time.time() - start_time
        if self.debug:
            print 'time taken to process readability %s' % time_taken
        if readability_text:
            if field_name not in content_extraction or (field_name in content_extraction and ep == _REPLACE):
                content_extraction[field_name] = readability_text
        return content_extraction

    def determine_extraction_policy(self, config):
        ep = _REPLACE
        if not config:
            return ep
        if _EXTRACTION_POLICY in config:
            ep = config[_EXTRACTION_POLICY]
        elif self.global_extraction_policy:
            ep = self.global_extraction_policy
        if ep and ep != _KEEP_EXISTING and ep != _REPLACE:
            raise ValueError('extraction_policy can either be {} or {}'.format(_KEEP_EXISTING, _REPLACE))
        return ep

    @staticmethod
    def _relevant_text_from_context(text_or_tokens, results, field_name):
        if results:
            tokens_len = len(text_or_tokens)
            if not isinstance(results, list):
                results = [results]
            for result in results:
                if 'context' in result:
                    start = int(result['context']['start'])
                    end = int(result['context']['end'])
                    if isinstance(text_or_tokens, basestring):
                        if start - 10 < 0:
                            new_start = 0
                        else:
                            new_start = start - 10
                        if end + 10 > tokens_len:
                            new_end = tokens_len
                        else:
                            new_end = end + 10
                        relevant_text = '<etk \'attribute\' = \'{}\'>{}</etk>'.format(field_name,
                                                                                      text_or_tokens[start:end].encode(
                                                                                          'utf-8'))
                        result['context']['text'] = '{} {} {}'.format(text_or_tokens[new_start:start].encode('utf-8'),
                                                                      relevant_text,
                                                                      text_or_tokens[end:new_end].encode('utf-8'))
                        result['context']['input'] = _TEXT
                    else:
                        if start - 5 < 0:
                            new_start = 0
                        else:
                            new_start = start - 5
                        if end + 5 > tokens_len:
                            new_end = tokens_len
                        else:
                            new_end = end + 5
                        relevant_text = '<etk \'attribute\' = \'{}\'>{}</etk>'.format(field_name,
                                                                                      ' '.join(text_or_tokens[
                                                                                               start:end]).encode(
                                                                                          'utf-8'))
                        result['context']['text'] = '{} {} {} '.format(
                            ' '.join(text_or_tokens[new_start:start]).encode('utf-8'),
                            relevant_text,
                            ' '.join(text_or_tokens[end:new_end]).encode('utf-8'))
                        result['context']['tokens_left'] = text_or_tokens[new_start:start]
                        result['context']['tokens_right'] = text_or_tokens[end:new_end]
                        result['context']['input'] = _TOKENS
        return results

    @staticmethod
    def sort_dict(dictionary):
        return collections.OrderedDict(sorted(dictionary.items()))

    @staticmethod
    def load_json_file(file_name):
        json_x = json.load(codecs.open(file_name, 'r'))
        return json_x

    def load_json(self, json_name):
        if json_name not in self.jobjs:
            self.jobjs[json_name] = self.load_json_file(self.get_pickle_file_name_from_config(json_name))
        return self.jobjs[json_name]

    def load_trie(self, file_name, case_sensitive=False):
        try:
            values = json.load(gzip.open(file_name), 'utf-8')
        except:
            values = None
        if not values:
            values = json.load(codecs.open(file_name), 'utf-8')

        if case_sensitive:
            trie = dictionary_extractor.populate_trie(map(lambda x: x, values))
        else:
            trie = dictionary_extractor.populate_trie(map(lambda x: x.lower(), values))
        return trie

    def load_dictionary(self, field_name, dict_name, case_sensitive):
        if field_name not in self.tries:
            self.tries[field_name] = self.load_trie(self.get_dict_file_name_from_config(dict_name), case_sensitive)

    def load_pickle_file(self, pickle_path):
        return pickle.load(open(pickle_path, 'rb'))

    def load_pickle(self, pickle_name):
        if pickle_name not in self.pickles:
            self.pickles[pickle_name] = self.load_pickle_file(self.get_pickle_file_name_from_config(pickle_name))
        return self.pickles[pickle_name]

    def table_data_extractor(self, d, config):
        result = self.table_data_extractor_(d, config)
        # return self._relevant_text_from_context([], result, config[_FIELD_NAME])
        return result

    def table_data_extractor_(self, d, config):
        sem_types = config['sem_types']
        sem_types = self.load_json(sem_types)
        method = config['method']
        model = config['model']
        if method == 'rule_based':
            model = self.load_json(model)
        else:
            model = self.load_pickle(model)
        tie = table_extractor.InformationExtraction(sem_types, method, model)
        results = tie.extract(d)
        return results

    def extract_using_dictionary(self, d, config):
        field_name = config[_FIELD_NAME]

        # case sensitivity is disabled by default
        case_sensitive = str(config[_CASE_SENSITIVE]).lower() == 'true' if _CASE_SENSITIVE in config else False

        # this method is self aware that it needs tokens as input
        if case_sensitive:
            tokens = d[_SIMPLE_TOKENS_ORIGINAL_CASE]
        else:
            tokens = d[_SIMPLE_TOKENS]

        if not tokens:
            return None
        if _DICTIONARY not in config:
            raise KeyError('No dictionary specified for {}'.format(field_name))

        self.load_dictionary(field_name, config[_DICTIONARY], case_sensitive)

        pre_process = None
        if _PRE_PROCESS in config and len(config[_PRE_PROCESS]) > 0:
            pre_process = self.string_to_lambda(config[_PRE_PROCESS][0])
        if not pre_process:
            pre_process = lambda x: x

        pre_filter = None
        if _PRE_FILTER in config and len(config[_PRE_FILTER]) > 0:
            pre_filter = self.string_to_lambda(config[_PRE_FILTER][0])
        if not pre_filter:
            pre_filter = lambda x: x

        post_filter = None
        if _PRE_FILTER in config and len(config[_PRE_FILTER]) > 0:
            post_filter = self.string_to_lambda(config[_PRE_FILTER][0])
        if not post_filter:
            post_filter = lambda x: isinstance(x, basestring)

        ngrams = int(config[_NGRAMS]) if _NGRAMS in config else 1

        joiner = config[_JOINER] if _JOINER in config else ' '

        return self._relevant_text_from_context(tokens, self._extract_using_dictionary(tokens, pre_process,
                                                                                                  self.tries[
                                                                                                      field_name],
                                                                                                  pre_filter,
                                                                                                  post_filter,
                                                                                                  ngrams, joiner),
                                                field_name)

    @staticmethod
    def _extract_using_dictionary(tokens, pre_process, trie, pre_filter, post_filter, ngrams, joiner):
        result = dictionary_extractor.extract_using_dictionary(tokens, pre_process=pre_process,
                                                               trie=trie,
                                                               pre_filter=pre_filter,
                                                               post_filter=post_filter,
                                                               ngrams=ngrams,
                                                               joiner=joiner)
        return result if result and len(result) > 0 else None

    def extract_website_domain(self, d, config):
        text = d[_TEXT]
        field_name = config[_FIELD_NAME]
        tld = self.extract_tld(text)
        results = {"value": tld}
        return self._relevant_text_from_context(d[_TEXT], results, field_name)

    def extract_using_regex(self, d, config):
        # this method is self aware that it needs the text, so look for text in the input d
        text = d[_TEXT]
        include_context = True
        if "include_context" in config and config['include_context'].lower() == 'false':
            include_context = False
        if "regex" not in config:
            raise KeyError('No regular expression found in {}'.format(json.dumps(config)))
        regex = config["regex"]
        flags = 0
        if "regex_options" in config:
            regex_options = config['regex_options']
            if not isinstance(regex_options, list):
                raise ValueError("regular expression options should be a list in {}".format(json.dumps(config)))
            for regex_option in regex_options:
                flags = flags | eval("re." + regex_option)
        if _PRE_FILTER in config:
            text = self.run_user_filters(d, config[_PRE_FILTER], config[_FIELD_NAME])

        result = self._extract_using_regex(text, regex, include_context, flags)
        # TODO ADD code to handle post_filters
        return self._relevant_text_from_context(d[_TEXT], result, config[_FIELD_NAME])

    @staticmethod
    def _extract_using_regex(text, regex, include_context, flags):
        try:
            result = regex_extractor.extract(text, regex, include_context, flags)
            return result if result and len(result) > 0 else None
        except Exception as e:
            print e
            return None

    def extract_using_custom_spacy(self, d, config, field_rules=None):
        if not field_rules:
            field_rules = self.load_json_file(self.get_spacy_field_rules_from_config(config[_SPACY_FIELD_RULES]))
        if not self.nlp:
            self.prep_spacy()

        # call the custom spacy extractor
        nlp_doc = self.nlp(d[_SIMPLE_TOKENS_ORIGINAL_CASE])
        results = self._relevant_text_from_context(d[_SIMPLE_TOKENS_ORIGINAL_CASE],
                                                   custom_spacy_extractor.extract(field_rules, nlp_doc, self.nlp),
                                                   config[_FIELD_NAME])
        return results

    def extract_using_spacy(self, d, config):
        field_name = config[_FIELD_NAME]
        if not self.nlp:
            self.prep_spacy()

        nlp_doc = self.nlp(d[_SIMPLE_TOKENS])
        self.load_matchers(field_name)
        results = None
        if field_name == _AGE:
            results = self._relevant_text_from_context(d[_SIMPLE_TOKENS],
                                                       spacy_age_extractor.extract(nlp_doc, self.matchers[_AGE]), _AGE)
        elif field_name == _POSTING_DATE:
            results = self._relevant_text_from_context(d[_SIMPLE_TOKENS],
                                                       spacy_date_extractor.extract(nlp_doc,
                                                                                    self.matchers[_POSTING_DATE]),
                                                       _POSTING_DATE)
            if _POST_FILTER in config:
                post_filters = config[_POST_FILTER]
                results = self.run_post_filters_results(results, post_filters)

        elif field_name == _SOCIAL_MEDIA:
            results = self._relevant_text_from_context(d[_SIMPLE_TOKENS],
                                                       spacy_social_media_extractor.extract(nlp_doc,
                                                                                            self.matchers[
                                                                                                _SOCIAL_MEDIA]),
                                                       _SOCIAL_MEDIA)
        elif field_name == _ADDRESS:
            results = self._relevant_text_from_context(d[_SIMPLE_TOKENS],
                                                       spacy_address_extractor.extract(nlp_doc,
                                                                                       self.matchers[_ADDRESS]),
                                                       _ADDRESS)
        return results

    def extract_from_landmark(self, doc, config):
        field_name = config[_FIELD_NAME]
        if _CONTENT_EXTRACTION not in doc:
            return None
        if _INFERLINK_EXTRACTIONS not in doc[_CONTENT_EXTRACTION]:
            return None
        results = list()
        inferlink_extraction = doc[_CONTENT_EXTRACTION][_INFERLINK_EXTRACTIONS]
        fields = None
        if _FIELDS in config:
            fields = config[_FIELDS]
        pre_filters = None
        if _PRE_FILTER in config:
            pre_filters = config[_PRE_FILTER]

        post_filters = None
        if _POST_FILTER in config:
            post_filters = config[_POST_FILTER]

        if fields:
            for field in fields:
                if field in inferlink_extraction:
                    d = inferlink_extraction[field]
                    if pre_filters:
                        # Assumption all pre_filters are lambdas
                        d[_TEXT] = self.run_user_filters(d, pre_filters, config[_FIELD_NAME])
                    result = None
                    if post_filters:
                        post_result = self.run_user_filters(d, post_filters, config[_FIELD_NAME])
                        if post_result:
                            result = self.handle_text_or_results(post_result)
                    else:
                        result = self.handle_text_or_results(d[_TEXT])
                    if result:
                        results.extend(result)
        else:
            for field in inferlink_extraction.keys():
                # The logic below: if the inferlink rules do not have semantic information in the field names returned,
                #                                                                                               too bad
                if field_name in field:
                    d = inferlink_extraction[field]
                    if pre_filters:
                        # Assumption all pre_filters are lambdas
                        d[_TEXT] = self.run_user_filters(d, pre_filters, config[_FIELD_NAME])

                    result = None
                    if post_filters:
                        post_result = self.run_user_filters(d, post_filters, config[_FIELD_NAME])
                        if post_result:
                            result = self.handle_text_or_results(post_result)
                    else:
                        result = self.handle_text_or_results(d[_TEXT])
                    if result:
                        results.extend(result)
        return results if len(results) > 0 else None

    def extract_phone(self, d, config):
        tokens = d[_SIMPLE_TOKENS]
        # source type as in text vs url #SHRUG
        source_type = config[_SOURCE_TYPE] if _SOURCE_TYPE in config else 'text'
        include_context = True
        output_format = _OBFUSCATION
        # if _PRE_FILTER in config:
        #     text = self.run_user_filters(d, config[_PRE_FILTER], config[_FIELD_NAME])
        return self._relevant_text_from_context(d[_SIMPLE_TOKENS],
                                                self._extract_phone(tokens, source_type, include_context,
                                                                    output_format), config[_FIELD_NAME])

    @staticmethod
    def _extract_phone(tokens, source_type, include_context, output_format):
        result = phone_extractor.extract(tokens, source_type, include_context, output_format)
        return result if result else None

    def extract_email(self, d, config):
        text = d[_TEXT]
        include_context = True
        if _INCLUDE_CONTEXT in config:
            include_context = config[_INCLUDE_CONTEXT].upper() == 'TRUE'
        if _PRE_FILTER in config:
            text = self.run_user_filters(d, config[_PRE_FILTER], config[_FIELD_NAME])
        return self._relevant_text_from_context(d[_TEXT], self._extract_email(text, include_context),
                                                config[_FIELD_NAME])

    @staticmethod
    def _extract_email(text, include_context):
        """
        A regular expression based function to extract emails from text
        :param text: The input text.
        :param include_context: True or False, will include context matched by the regular expressions.
        :return: An object, with extracted email and/or context.
        """
        return email_extractor.extract(text, include_context)

    def extract_price(self, d, config):
        text = d[_TEXT]
        if _PRE_FILTER in config:
            text = self.run_user_filters(d, config[_PRE_FILTER], config[_FIELD_NAME])
        return self._relevant_text_from_context(d[_TEXT], self._extract_price(text), config[_FIELD_NAME])

    @staticmethod
    def _extract_price(text):
        return price_extractor.extract(text)

    def extract_height(self, d, config):
        text = d[_TEXT]
        if _PRE_FILTER in config:
            text = self.run_user_filters(d, config[_PRE_FILTER], config[_FIELD_NAME])
        return self._relevant_text_from_context(d[_TEXT], self._extract_height(text), config[_FIELD_NAME])

    @staticmethod
    def _extract_height(text):
        return height_extractor.extract(text)

    def extract_weight(self, d, config):
        text = d[_TEXT]
        if _PRE_FILTER in config:
            text = self.run_user_filters(d, config[_PRE_FILTER], config[_FIELD_NAME])
        return self._relevant_text_from_context(d[_TEXT], self._extract_weight(text), config[_FIELD_NAME])

    @staticmethod
    def _extract_weight(text):
        return weight_extractor.extract(text)

    def extract_address(self, d, config):
        text = d[_TEXT]
        if _PRE_FILTER in config:
            text = self.run_user_filters(d, config[_PRE_FILTER], config[_FIELD_NAME])
        return self._relevant_text_from_context(d[_TEXT], self._extract_address(text), config[_FIELD_NAME])

    @staticmethod
    def _extract_address(text):
        return address_extractor.extract(text)

    def extract_age(self, d, config):
        text = d[_TEXT]
        if _PRE_FILTER in config:
            text = self.run_user_filters(d, config[_PRE_FILTER], config[_FIELD_NAME])
        return self._relevant_text_from_context(d[_TEXT], self._extract_age(text), config[_FIELD_NAME])

    @staticmethod
    def _extract_age(text):
        return age_extractor.extract(text)

    def extract_review_id(self, d, config):
        text = d[_TEXT]
        if _PRE_FILTER in config:
            text = self.run_user_filters(d, config[_PRE_FILTER], config[_FIELD_NAME])
        return self._relevant_text_from_context(d[_TEXT], self._extract_review_id(text), config[_FIELD_NAME])

    @staticmethod
    def _extract_review_id(text):
        return review_id_extractor.extract(text)

    @staticmethod
    def handle_text_or_results(x):
        if isinstance(x, basestring):
            o = dict()
            o['value'] = x
            return [o]
        if isinstance(x, dict):
            return [x]
        if isinstance(x, list):
            return x
        return None

    def run_user_filters(self, d, filters, field_name):
        result = None
        if not isinstance(filters, list):
            filters = [filters]
        try:
            for text_filter in filters:
                try:
                    f = getattr(self, text_filter)
                    if f:
                        result = f(d, {_FIELD_NAME: field_name})
                except Exception as e:
                    result = None
                if not result:
                    result = Core.string_to_lambda(text_filter)(d[_TEXT])
        except Exception as e:
            print 'Error {} in {}'.format(e, 'run_user_filters')
        return result

    def run_post_filters_results(self, results, post_filters):
        if results:
            if not isinstance(results, list):
                results = [results]
            if not isinstance(post_filters, list):
                post_filters = [post_filters]
            out_results = list()
            for post_filter in post_filters:
                try:
                    f = getattr(self, post_filter)
                except Exception as e:
                    raise 'Exception: {}, no function {} defined in core.py'.format(e, post_filter)

                for result in results:
                    val = f(result['value'])
                    if val:
                        result['value'] = val
                        out_results.append(result)
            return out_results if len(out_results) > 0 else None

    @staticmethod
    def string_to_lambda(s):
        try:
            return lambda x: eval(s)
        except:
            print 'Error while converting {} to lambda'.format(s)
            return None

    @staticmethod
    def extract_readability(document, options=None):
        e = ReadabilityExtractor()
        return e.extract(document, options)

    def extract_title(self, html_content, options=None):
        if html_content:
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
        return None

    @staticmethod
    def extract_crftokens(text, options=None, lowercase=True):
        t = TokenizerExtractor(recognize_linebreaks=True, create_structured_tokens=True)
        return t.extract(text, lowercase)

    @staticmethod
    def crftokens_to_lower(crf_tokens):
        lower_crf = copy.deepcopy(crf_tokens)
        for tk in lower_crf:
            tk['value'] = tk['value'].lower()
        return lower_crf

    @staticmethod
    def extract_tokens_from_crf(crf_tokens):
        return [tk['value'] for tk in crf_tokens]

    @staticmethod
    def extract_tokens_faithful(text, options=None):
        ft = FaithfulTokenizerExtractor(recognize_linebreaks=True, create_structured_tokens=True)
        return ft.extract(text)

    @staticmethod
    def extract_tokens_from_faithful(faithful_tokens):
        return [tk['value'] for tk in faithful_tokens]

    @staticmethod
    def filter_tokens(original_tokens, config):
        # config contains a list of types of tokens to be removed
        # [alphabet, digit, emoji, punctuation, html, html_entity, break]
        ft = FaithfulTokenizerExtractor(recognize_linebreaks=True, create_structured_tokens=True)
        ft.faithful_tokens = original_tokens
        # Return Tokens object which contains - tokens, reverse_map attributes
        # The object also has a method get_original_index() to retrieve index in faithful tokens
        return ft.filter_tokens(config)

    def extract_table(self, d, config):
        cl_tables = False
        model = None,
        sem_types = []
        if _CONFIG in config:
            config = config[_CONFIG]
            if config['classify_tables'] == 'yes':
                cl_tables = True
                model = config['classification_model']
                sem_types = config['sem_types']
                sem_types = self.load_json(sem_types)
                model = self.load_pickle(model)
        te = table_extractor.TableExtraction(cl_tables, sem_types, model)
        return te.extract(d)

    # def extract_stock_tickers(self, doc):
    #     return extract_stock_tickers(doc)

    # def extract_spacy(self, doc):
    #     return spacy_extractor.spacy_extract(doc)

    @staticmethod
    def extract_landmark(html, url, extraction_rules, threshold=0.5):
        return landmark_extraction.extract(html, url, extraction_rules, threshold)

    def prep_spacy(self):
        self.nlp = spacy.load('en')
        self.old_tokenizer = self.nlp.tokenizer
        self.nlp.tokenizer = lambda tokens: self.old_tokenizer.tokens_from_list(tokens)

    def load_matchers(self, field_name=None):
        if field_name:
            if field_name == _AGE:
                if _AGE not in self.matchers:
                    self.matchers[_AGE] = spacy_age_extractor.load_age_matcher(self.nlp)
            if field_name == _POSTING_DATE:
                if _POSTING_DATE not in self.matchers:
                    self.matchers[_POSTING_DATE] = spacy_date_extractor.load_date_matcher(self.nlp)
            if field_name == _SOCIAL_MEDIA:
                if _SOCIAL_MEDIA not in self.matchers:
                    self.matchers[_SOCIAL_MEDIA] = spacy_social_media_extractor.load_social_media_matcher(self.nlp)
            if field_name == _ADDRESS:
                if _ADDRESS not in self.matchers:
                    self.matchers[_ADDRESS] = spacy_address_extractor.load_address_matcher(self.nlp)

    @staticmethod
    def create_list_data_extraction(data_extraction, field_name, method=_EXTRACT_USING_DICTIONARY):
        out = list()
        if data_extraction:
            if field_name in data_extraction:
                extractions = data_extraction[field_name]
                if method in extractions:
                    out = Core.get_value_list_from_results(extractions[method]['results'])
        return out

    @staticmethod
    def get_value_list_from_results(results):
        out = list()
        if results:
            for result in results:
                out.append(result['value'])
        return out

    def extract_country_url(self, d, config):
        if not self.country_code_dict:
            try:
                self.country_code_dict = self.load_json_file(self.get_dict_file_name_from_config('country_code'))
            except:
                raise '{} dictionary missing from resources'.format('country_code')

        tokens_url = d[_SIMPLE_TOKENS]
        return self._relevant_text_from_context(tokens_url,
                                                url_country_extractor.extract(tokens_url, self.country_code_dict),
                                                config[_FIELD_NAME])

    def geonames_lookup(self, d, config):
        field_name = config[_FIELD_NAME]

        if not self.geonames_dict:
            try:
                self.geonames_dict = self.load_json_file(self.get_dict_file_name_from_config(_GEONAMES))
            except Exception as e:
                raise '{} dictionary missing from resources'.format(_GEONAMES)

        if _CITY_NAME in d[_KNOWLEDGE_GRAPH]:
            cities = d[_KNOWLEDGE_GRAPH][_CITY_NAME].keys()
        else:
            return None
        populated_places = geonames_extractor.get_populated_places(cities, self.geonames_dict)

        # results = geonames_extractor.get_country_from_populated_places(populated_places)

        # if results:
        #     self.create_knowledge_graph(d, _COUNTRY , results)

        return populated_places

    @staticmethod
    def parse_date(d, config={}):
        if isinstance(d, basestring):
            return Core.spacy_parse_date(d)
        else:
            try:
                return date_parser.convert_to_iso_format(date_parser.parse_date(d[_TEXT]))
            except:
                return None

    @staticmethod
    def spacy_parse_date(str_date):
        try:
            return date_parser.convert_to_iso_format(date_parser.parse_date(str_date))
        except:
            return None

    @staticmethod
    def filter_age(d, config):
        text = d[_TEXT]
        try:
            text = text.replace('\n', '')
            text = text.replace('\t', '')
            num = int(text)
            return num if 18 <= num <= 65 else None
        except:
            pass
        return None

    def country_from_states(self, d, config):
        if not self.state_to_country_dict:
            try:
                self.state_to_country_dict = self.load_json_file(self.get_dict_file_name_from_config(_STATE_TO_COUNTRY))
            except Exception as e:
                raise '{} dictionary missing from resources'.format(_STATE_TO_COUNTRY)

        if _STATE in d[_KNOWLEDGE_GRAPH]:
            states = d[_KNOWLEDGE_GRAPH][_STATE].keys()
        else:
            return None

        return geonames_extractor.get_country_from_states(states, self.state_to_country_dict)

    def country_feature(self, d, config):
        return country_classifier.calc_country_feature(d[_KNOWLEDGE_GRAPH], self.state_to_country_dict)

    def create_city_state_country_triple(self, d, config):
        if not self.state_to_codes_lower_dict:
            try:
                self.state_to_codes_lower_dict = self.load_json_file(
                    self.get_dict_file_name_from_config(_STATE_TO_CODES_LOWER))
            except Exception as e:
                raise ValueError('{} dictionary missing from resources'.format(_STATE_TO_CODES_LOWER))
        if not self.populated_cities:
            try:
                self.populated_cities = self.load_json_file(self.get_dict_file_name_from_config(_POPULATED_CITIES))
            except Exception as e:
                raise ValueError('{} dictionary missing from resources'.format(_POPULATED_CITIES))

        try:
            priori_lst = ['city_state_together', 'city_state_code_together',
                          'city_country_together', 'city_state_separate',
                          'city_country_separate', 'city_state_code_separate']
            results = [[] for i in range(len(priori_lst)+1)]
            knowledge_graph = d[_KNOWLEDGE_GRAPH]
            if "populated_places" in knowledge_graph:
                pop_places = knowledge_graph["populated_places"]
                for place in pop_places:
                    city_state_together_count = 0
                    city_state_separate_count = 0
                    city_state_code_together_count = 0
                    city_state_code_separate_count = 0
                    city_country_together_count = 0
                    city_country_separate_count = 0
                    city = pop_places[place][0]["value"]
                    state = pop_places[place][0]["metadata"]["state"]
                    state = "" if not state else state
                    country = pop_places[place][0]["metadata"]["country"]
                    country = "" if not country else country
                    if state in self.state_to_codes_lower_dict:
                        state_code = self.state_to_codes_lower_dict[state]
                    else:
                        state_code = None

                    cities = []
                    if "city_name" in knowledge_graph:
                        if city in knowledge_graph["city_name"]:
                            city_lst = knowledge_graph["city_name"][city]
                            for each_city in city_lst:
                                if "context" in each_city:
                                    cities.append((each_city["origin"]["segment"],
                                        each_city["context"]["start"], each_city["context"]["end"]))
                            if city_lst:
                                document_id = each_city["origin"]["document_id"]
                            else:
                                document_id = ""

                    states = []
                    if country == "united states":
                        if "state" in knowledge_graph:
                            if state in knowledge_graph["state"]:
                                state_lst = knowledge_graph["state"][state]
                                for each_state in state_lst:
                                    if "context" in each_state:
                                        states.append((each_state["origin"]["segment"],
                                                       each_state["context"]["start"], each_state["context"]["end"]))

                    countries = []
                    if "country" in knowledge_graph:
                        if country in knowledge_graph["country"]:
                            country_lst = knowledge_graph["country"][country]
                            for each_country in country_lst:
                                if "context" in each_country:
                                    countries.append((each_country["origin"]["segment"],
                                                      each_country["context"]["start"], each_country["context"]["end"]))

                    state_codes = []
                    if country == "united states":
                        if state_code:
                            if "states_usa_codes" in knowledge_graph:
                                if state_code in knowledge_graph["states_usa_codes"]:
                                    state_code_lst = knowledge_graph["states_usa_codes"][state_code]
                                    for each_state_code in state_code_lst:
                                        if "context" in each_state_code:
                                            state_codes.append((each_state_code["origin"]["segment"],
                                                                each_state_code["context"]["start"],
                                                                each_state_code["context"]["end"]))

                    if cities:
                        segments = []
                        for a_city in cities:
                            for a_state in states:
                                if a_city[0] == a_state[0] and a_city[1] != a_state[1] and (
                                        abs(a_city[2] - a_state[1]) < 3 or abs(a_city[1] - a_state[2]) < 3):
                                    city_state_together_count += 1
                                    if a_city[0] not in segments:
                                        segments.append(a_city[0])
                                else:
                                    city_state_separate_count += 1
                            for a_state_code in state_codes:
                                if a_city[0] == a_state_code[0] and a_city[1] != a_state_code[1] and a_state_code[1] - \
                                        a_city[2] < 3 and a_state_code[1] - a_city[2] > 0:
                                    city_state_code_together_count += 1
                                    if a_city[0] not in segments:
                                        segments.append(a_city[0])
                                else:
                                    city_state_code_separate_count += 1
                            for a_country in countries:
                                if a_city[0] == a_country[0] and a_city[1] != a_country[1] and (
                                        abs(a_city[2] - a_country[1]) < 5 or abs(a_city[1] - a_country[2]) < 3):
                                    city_country_together_count += 1
                                    if a_city[0] not in segments:
                                        segments.append(a_city[0])
                                else:
                                    city_country_separate_count += 1

                        result = copy.deepcopy(pop_places[place][0])
                        result['origin'] = dict()
                        result['origin']['document_id'] = document_id
                        result['origin']['method'] = 'create_city_state_country_triple'
                        result['metadata']['city_state_together'] = city_state_together_count
                        result['metadata']['city_state_separate'] = city_state_separate_count
                        result['metadata']['city_state_code_together'] = city_state_code_together_count
                        result['metadata']['city_state_code_separate'] = city_state_code_separate_count
                        result['metadata']['city_country_together'] = city_country_together_count
                        result['metadata']['city_country_separate'] = city_country_separate_count
                        for priori_idx, counter in enumerate(priori_lst):
                            if country == "united states":
                                result_value = city + ',' + state
                            else:
                                result_value = city + ',' + country
                            result['key'] = city + ':' + state + ':' + country + ':' + str(
                                result['metadata']['longitude']) + ':' + str(result['metadata']['latitude'])
                            if result['metadata'][counter] > 0:
                                if priori_idx < 3:
                                    result['value'] = result_value + "-1.0"
                                    result['origin']['score'] = 1.0
                                    result['origin']['segment'] = counter + ' in'
                                    for segment in segments:
                                        result['origin']['segment'] = result['origin']['segment'] + ' ' + segment
                                elif priori_idx < 5:
                                    result['value'] = result_value + "-0.8"
                                    result['origin']['score'] = 0.8
                                    result['origin']['segment'] = counter + ' in somewhere'
                                else:
                                    result['value'] = result_value + "-0.3"
                                    result['origin']['score'] = 0.3
                                    result['origin']['segment'] = counter + ' in somewhere'
                                results[priori_idx].append(result)
                                break
                            else:
                                if isinstance(self.populated_cities, dict):
                                    if priori_idx == 5 and city in self.populated_cities:
                                        if self.populated_cities[city]["country"] == country:
                                            if "state" in self.populated_cities[city]:
                                                if self.populated_cities[city]["state"] == state:
                                                    result['value'] = result_value + "-0.1"
                                                    result['origin']['score'] = 0.1
                                                    result['origin']['segment'] = 'none'
                                                    results[priori_idx+1].append(result)
                                            else:
                                                result['value'] = result_value + "-0.1"
                                                result['origin']['score'] = 0.1
                                                result['origin']['segment'] = 'none'
                                                results[priori_idx + 1].append(result)
                                else:
                                    if priori_idx == 5 and city in self.populated_cities:
                                        result['value'] = result_value + "-0.1"
                                        result['origin']['score'] = 0.1
                                        result['origin']['segment'] = 'none'
                                        results[priori_idx + 1].append(result)

            return_result = None
            for priori in range(len(priori_lst) + 1):
                if results[priori]:
                    if priori < 3:
                        return_result = results[priori]
                        break
                    else:
                        high_pop = 0
                        high_idx = 0
                        for idx, a_result in enumerate(results[priori]):
                            if a_result['metadata']['population'] >= high_pop:
                                high_pop = a_result['metadata']['population']
                                high_idx = idx
                        return_result = [results[priori][high_idx]]
                        break

            return return_result

        except Exception as e:
            print e
            return None
