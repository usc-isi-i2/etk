# import all extractors
from spacy_extractors import age_extractor as spacy_age_extractor
from spacy_extractors import date_extractor as spacy_date_extractor
from data_extractors import spacy_extractor
from data_extractors import landmark_extraction
from data_extractors import dictionary_extractor
from data_extractors import regex_extractor
from data_extractors import height_extractor
from data_extractors import weight_extractor
from data_extractors import address_extractor
from data_extractors import age_extractor
from data_extractors import table_extractor 
from data_extractors.digPhoneExtractor import phone_extractor
from data_extractors.digEmailExtractor import email_extractor
from data_extractors.digPriceExtractor import price_extractor
from data_extractors.digReviewIDExtractor import review_id_extractor
from structured_extractors import ReadabilityExtractor, TokenizerExtractor
import json
import gzip
import re
import spacy
import codecs
from jsonpath_rw import parse, jsonpath
import time
import collections
import numbers

_KNOWLEDGE_GRAPH = "knowledge_graph"
_EXTRACTION_POLICY = 'extraction_policy'
_KEEP_EXISTING = 'keep_existing'
_REPLACE = 'replace'
_ERROR_HANDLING = 'error_handling'
_IGNORE_EXTRACTION = 'ignore_extraction'
_IGNORE_DOCUMENT = 'ignore_document'
_RAISE_ERROR = 'raise_error'
_CITY = 'city'
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
_RESOURCES = 'resources'
_DATA_EXTRACTION = 'data_extraction'
_FIELDS = 'fields'
_EXTRACTORS = 'extractors'
_TOKENS = 'tokens'
_SIMPLE_TOKENS = 'simple_tokens'
_TEXT = 'text'
_DICTIONARY = 'dictionary'
_NGRAMS = 'ngrams'
_JOINER = 'joiner'
_PRE_FILTER = 'pre_filter'
_POST_FILTER = 'post_filter'
_PRE_PROCESS = "pre_process"
_TABLE = "table"

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
_SEGMENT_READABILITY_STRICT = "readability_strict"
_SEGMENT_READABILITY_RELAXED = "readability_relaxed"
_SEGMENT_OTHER = "other_segment"

_METHOD_GUROBI = "gurobi"
_METHOD_INFERLINK = "inferlink"
_METHOD_OTHER = "other_method"

_SOURCE_TYPE = "source_type"
_OBFUSCATION = "obfuscation"

_INCLUDE_CONTEXT = "include_context"


class Core(object):

    def __init__(self, extraction_config=None, debug=False, load_spacy=False):
        self.extraction_config = extraction_config
        self.debug = debug
        self.html_title_regex = r'<title>(.*)?</title>'
        self.tries = dict()
        self.global_extraction_policy = None
        self.global_error_handling = None
        # to make sure we do not parse json_paths more times than needed, we define the following 2 properties
        self.content_extraction_path = None
        self.data_extraction_path = dict()
        if load_spacy:
            self.load_matchers()
        else:
            self.nlp = None

    """ Define all API methods """

    def process(self, doc, create_knowledge_graph=False):
        if self.extraction_config:
            if _EXTRACTION_POLICY in self.extraction_config:
                self.global_extraction_policy = self.extraction_config[_EXTRACTION_POLICY]
            if _ERROR_HANDLING in self.extraction_config:
                self.global_error_handling = self.extraction_config[_ERROR_HANDLING]

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
                            re_extractors = extractors[extractor]
                            if isinstance(re_extractors, dict):
                                re_extractors = [re_extractors]
                            for re_extractor in re_extractors:
                                doc[_CONTENT_EXTRACTION] = self.run_readability(doc[_CONTENT_EXTRACTION],
                                                                                matches[index].value, re_extractor)
                        elif extractor == _TITLE:
                            doc[_CONTENT_EXTRACTION] = self.run_title(doc[_CONTENT_EXTRACTION], matches[index].value,
                                                                      extractors[extractor])
                        elif extractor == _LANDMARK:
                            doc[_CONTENT_EXTRACTION] = self.run_landmark(doc[_CONTENT_EXTRACTION], matches[index].value,
                                                                         extractors[extractor], doc[_URL])
                        elif extractor == _TABLE:
                            doc[_CONTENT_EXTRACTION] = self.run_table_extractor(doc[_CONTENT_EXTRACTION], 
                                                                        matches[index].value, extractors[extractor])
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
                                if _TOKENS not in match.value:
                                    match.value[_TOKENS] = self.extract_crftokens(match.value[_TEXT])
                                if _SIMPLE_TOKENS not in match.value:
                                    match.value[_SIMPLE_TOKENS] = self.extract_tokens_from_crf(match.value[_TOKENS])
                                fields = de_config[_FIELDS]
                                for field in fields.keys():
                                    """
                                        Special case for inferlink extractions:
                                        For eg, We do not want to extract name from inferlink_posting-date #DUH
                                    """
                                    run_extractor = True
                                    full_path = str(match.full_path)
                                    segment = self.determine_segment(full_path)
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
                                                except Exception as e:
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

                                                                results = foo(doc, extractors[extractor][_CONFIG])
                                                                if results:
                                                                    self.add_data_extraction_results(match.value, field,
                                                                                                     extractor,
                                                                                                self.add_origin_info(
                                                                                                         results,
                                                                                                         method,
                                                                                                         segment,
                                                                                                         score))
                                                                    if create_knowledge_graph:
                                                                        self.create_knowledge_graph(doc, field, results)
                                                    else:
                                                        if self.check_if_run_extraction(match.value, field,
                                                                                        extractor,
                                                                                        ep):
                                                            results = foo(match.value,
                                                                          extractors[extractor][_CONFIG])
                                                            if results:
                                                                self.add_data_extraction_results(match.value, field,
                                                                                                 extractor,
                                                                                            self.add_origin_info(
                                                                                                     results,
                                                                                                     method,
                                                                                                     segment,
                                                                                                     score))
                                                                if create_knowledge_graph:
                                                                    self.create_knowledge_graph(doc, field, results)

        if _KNOWLEDGE_GRAPH in doc and doc[_KNOWLEDGE_GRAPH]:
            doc[_KNOWLEDGE_GRAPH] = self.reformat_knowledge_graph(doc[_KNOWLEDGE_GRAPH])
        return doc

    @staticmethod
    def create_knowledge_graph(doc, field_name, extractions):
        if _KNOWLEDGE_GRAPH not in doc:
            doc[_KNOWLEDGE_GRAPH] = dict()

        if field_name not in doc[_KNOWLEDGE_GRAPH]:
            doc[_KNOWLEDGE_GRAPH][field_name] = dict()

        for extraction in extractions:
            key = extraction['value']
            if isinstance(key, str) or isinstance(key, numbers.Number):
                key = str(key).strip().lower()

            if 'metadata' in extraction:
                sorted_metadata = Core.sort_dict(extraction['metadata'])
                for k, v in sorted_metadata.iteritems():
                    if v and v.strip() != '':
                        key += '-' + str(k) + ':' + str(v)

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
                o['confidence'] = 1000
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
            segment = _SEGMENT_READABILITY_STRICT
        elif _CONTENT_RELAXED in json_path:
            segment = _SEGMENT_READABILITY_RELAXED
        elif _TITLE in json_path:
            segment = _TITLE
        elif _URL in json_path:
            segment = _URL
        return segment

    @staticmethod
    def add_origin_info(results, method, segment, score):
        if results:
            for result in results:
                o = dict()
                o['segment'] = segment
                o['method'] = method
                o['score'] = score
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
            content_extraction[field_name] = dict()
            start_time = time.time()
            ifl_extractions = Core.extract_landmark(html, url, extraction_rules, pct)
            time_taken = time.time() - start_time
            if self.debug:
                print 'time taken to process landmark %s' % time_taken
            if ifl_extractions:
                for key in ifl_extractions:
                    o = dict()
                    o[key] = dict()
                    o[key]['text'] = ifl_extractions[key]
                    content_extraction[field_name].update(o)
        return content_extraction

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

    def run_title(self, content_extraction, html, title_config):
        field_name = title_config[_FIELD_NAME] if _FIELD_NAME in title_config else _TITLE
        ep = self.determine_extraction_policy(title_config)
        if field_name not in content_extraction or (field_name in content_extraction and ep == _REPLACE):
            start_time = time.time()
            content_extraction[field_name] = self.extract_title(html)
            time_taken = time.time() - start_time
            if self.debug:
                print 'time taken to process title %s' % time_taken
        return content_extraction

    def run_table_extractor(self, content_extraction, html, table_config):
        field_name = table_config[_FIELD_NAME] if _FIELD_NAME in table_config else _TABLE
        ep = self.determine_extraction_policy(table_config)
        if field_name not in content_extraction or (field_name in content_extraction and ep == _REPLACE):
            start_time = time.time()
            content_extraction[field_name] = self.extract_table(html)
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
    def _relevant_text_from_context(text_or_tokens, results):
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
                            start = 0
                        else:
                            start -= 10
                        if end + 10 > tokens_len:
                            end = tokens_len
                        else:
                            end += 10
                        result['context']['text'] = text_or_tokens[start:end]
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
                        result['context']['text'] = ' '.join(text_or_tokens[new_start:new_end])
                        result['context']['tokens_left'] = text_or_tokens[new_start:start]
                        result['context']['tokens_right'] =  text_or_tokens[end:new_end]
                        result['context']['input'] = _TOKENS
        return results


    @staticmethod
    def sort_dict(dictionary):
        return collections.OrderedDict(sorted(dictionary.items()))

    @staticmethod
    def load_json_file(file_name):
        json_x = json.load(codecs.open(file_name, 'r'))
        return json_x

    def load_trie(self, file_name):
        values = json.load(gzip.open(file_name), 'utf-8')
        trie = dictionary_extractor.populate_trie(map(lambda x: x.lower(), values))
        return trie

    def load_dictionary(self, field_name, dict_name):
        if field_name not in self.tries:
            self.tries[field_name] = self.load_trie(self.get_dict_file_name_from_config(dict_name))

    def extract_using_dictionary(self, d, config):
        field_name = config[_FIELD_NAME]
        # this method is self aware that it needs tokens as input
        tokens = d[_SIMPLE_TOKENS]

        if not tokens:
            return None
        if _DICTIONARY not in config:
            raise KeyError('No dictionary specified for {}'.format(field_name))

        self.load_dictionary(field_name, config[_DICTIONARY])

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

        return self._relevant_text_from_context(d[_SIMPLE_TOKENS], self._extract_using_dictionary(tokens, pre_process,
                                                                                                  self.tries[
                                                                                                      field_name],
                                                                                                  pre_filter,
                                                                                                  post_filter,
                                                                                                  ngrams, joiner))

    @staticmethod
    def _extract_using_dictionary(tokens, pre_process, trie, pre_filter, post_filter, ngrams, joiner):
        result = dictionary_extractor.extract_using_dictionary(tokens, pre_process=pre_process,
                                                               trie=trie,
                                                               pre_filter=pre_filter,
                                                               post_filter=post_filter,
                                                               ngrams=ngrams,
                                                               joiner=joiner)
        return result if result and len(result) > 0 else None

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
            text = self.run_user_filters(d, config[_PRE_FILTER])

        result = self._extract_using_regex(text, regex, include_context, flags)
        # TODO ADD code to handle post_filters

        return self._relevant_text_from_context(d[_TEXT], result)

    @staticmethod
    def _extract_using_regex(text, regex, include_context, flags):
        try:
            result = regex_extractor.extract(text, regex, include_context, flags)
            return result if result and len(result) > 0 else None
        except Exception as e:
            print e
            return None

    def extract_using_spacy(self, d, config):
        field = config[_FIELD_NAME]
        if _SPACY_EXTRACTION not in d:
            d[_SPACY_EXTRACTION] = self.run_spacy_extraction(d)
        return d[_SPACY_EXTRACTION][field] if field in d[_SPACY_EXTRACTION] else None

    def run_spacy_extraction(self, d):
        if not self.nlp:
            self.load_matchers()
        spacy_extractions = dict()
        if len(d[_SIMPLE_TOKENS]) > 0:
            spacy_extractions[_POSTING_DATE] = self._relevant_text_from_context(d[_SIMPLE_TOKENS], spacy_date_extractor.
                                                                            extract(self.nlp, self.matchers['date'],
                                                                                    d[_SIMPLE_TOKENS]))
        if d[_TEXT].strip() != '':
            spacy_extractions[_AGE] = self._relevant_text_from_context(d[_TEXT],
                                                                   spacy_age_extractor.extract(d[_TEXT], self.nlp,
                                                                                               self.matchers['age']))
        return spacy_extractions

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
                        d[_TEXT] = self.run_user_filters(d, pre_filters)
                    post_result = None
                    if post_filters:
                        post_result = self.run_user_filters(d, post_filters)
                    result = self.handle_text_or_results(post_result) if post_result else self.handle_text_or_results(
                        d[_TEXT])
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
                        d[_TEXT] = self.run_user_filters(d, pre_filters)
                    post_result = None
                    if post_filters:
                        post_result = self.run_user_filters(d, post_filters)
                    result = self.handle_text_or_results(post_result) if post_result else self.handle_text_or_results(
                        d[_TEXT])
                    if result:
                        results.extend(result)

        return results if len(results) > 0 else None

    def extract_phone(self, d, config):
        tokens = d[_SIMPLE_TOKENS]
        # source type as in text vs url #SHRUG
        source_type = config[_SOURCE_TYPE] if _SOURCE_TYPE in config else 'text'
        include_context = True
        output_format= _OBFUSCATION
        if _PRE_FILTER in config:
            text = self.run_user_filters(d, config[_PRE_FILTER])
        return self._relevant_text_from_context(d[_SIMPLE_TOKENS],
                                                self._extract_phone(tokens, source_type, include_context,
                                                                    output_format))

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
            text = self.run_user_filters(d, config[_PRE_FILTER])
        return self._relevant_text_from_context(d[_TEXT], self._extract_email(text, include_context))

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
            text = self.run_user_filters(d, config[_PRE_FILTER])
        return self._relevant_text_from_context(d[_TEXT], self._extract_price(text))

    @staticmethod
    def _extract_price(text):
        return price_extractor.extract(text)

    def extract_height(self, d, config):
        text = d[_TEXT]
        if _PRE_FILTER in config:
            text = self.run_user_filters(d, config[_PRE_FILTER])
        return self._relevant_text_from_context(d[_TEXT], self._extract_height(text))

    @staticmethod
    def _extract_height(text):
        return height_extractor.extract(text)

    def extract_weight(self, d, config):
        text = d[_TEXT]
        if _PRE_FILTER in config:
            text = self.run_user_filters(d, config[_PRE_FILTER])
        return self._relevant_text_from_context(d[_TEXT], self._extract_weight(text))

    @staticmethod
    def _extract_weight(text):
        return weight_extractor.extract(text)

    def extract_address(self, d, config):
        text = d[_TEXT]
        if _PRE_FILTER in config:
            text = self.run_user_filters(d, config[_PRE_FILTER])
        return self._relevant_text_from_context(d[_TEXT], self._extract_address(text))

    @staticmethod
    def _extract_address(text):
        return address_extractor.extract(text)

    def extract_age(self, d, config):
        text = d[_TEXT]
        if _PRE_FILTER in config:
            text = self.run_user_filters(d, config[_PRE_FILTER])
        return self._relevant_text_from_context(d[_TEXT],self._extract_age(text))

    @staticmethod
    def _extract_age(text):
        return age_extractor.extract(text)

    def extract_review_id(self, d, config):
        text = d[_TEXT]
        if _PRE_FILTER in config:
            text = self.run_user_filters(d, config[_PRE_FILTER])
        return self._relevant_text_from_context(d[_TEXT], self._extract_review_id(text))

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

    def run_user_filters(self, d, filters):
        result = None
        if not isinstance(filters, list):
            filters = [filters]
        try:
            for text_filter in filters:
                try:
                    f = getattr(self, text_filter)
                    if f:
                        result = f(d, {})
                except Exception as e:
                    result = None

                if not result:
                    result = Core.string_to_lambda(text_filter)(d[_TEXT])
        except Exception as e:
            print 'Error {} in {}'.format(e, 'run_user_filters')
        return result

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

    @staticmethod
    def extract_crftokens(text, options=None):
        t = TokenizerExtractor(recognize_linebreaks=True, create_structured_tokens=True)
        return t.extract(text)

    @staticmethod
    def extract_tokens_from_crf(crf_tokens):
        return [tk['value'] for tk in crf_tokens]

    def extract_table(self, html_doc):
        return table_extractor.extract(html_doc)


    def extract_stock_tickers(self, doc):
        return extract_stock_tickers(doc)

    def extract_spacy(self, doc):
        return spacy_extractor.spacy_extract(doc)

    @staticmethod
    def extract_landmark(html, url, extraction_rules, threshold=0.5):
        return landmark_extraction.extract(html, url, extraction_rules, threshold)

    def load_matchers(self):
        self.nlp = spacy.load('en')
        self.spacy_tokenizer = self.nlp.tokenizer
        matchers = dict()

        # Load date_extractor matcher
        matchers['date'] = spacy_date_extractor.load_date_matcher(self.nlp)

        # Load age_extractor matcher
        matchers['age'] = spacy_age_extractor.load_age_matcher(self.nlp)
        self.matchers = matchers

