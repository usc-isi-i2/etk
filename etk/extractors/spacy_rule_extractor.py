from typing import List, Dict
from etk.extractor import Extractor, InputType
from etk.etk_extraction import Extraction
from etk.tokenizer import Tokenizer
from spacy.matcher import Matcher
from spacy import attrs
import copy
import itertools


FLAG_DICT = {
    20: attrs.FLAG20,
    21: attrs.FLAG21,
    22: attrs.FLAG22,
    23: attrs.FLAG23,
    24: attrs.FLAG24,
    25: attrs.FLAG25,
    26: attrs.FLAG26,
    27: attrs.FLAG27,
    28: attrs.FLAG28,
    29: attrs.FLAG29,
    30: attrs.FLAG30,
    31: attrs.FLAG31,
    32: attrs.FLAG32,
    33: attrs.FLAG33,
    34: attrs.FLAG34,
    35: attrs.FLAG35,
    36: attrs.FLAG36,
    37: attrs.FLAG37,
    38: attrs.FLAG38,
    39: attrs.FLAG39,
    40: attrs.FLAG40,
    41: attrs.FLAG41,
    42: attrs.FLAG42,
    43: attrs.FLAG43,
    44: attrs.FLAG44,
    45: attrs.FLAG45,
    46: attrs.FLAG46,
    47: attrs.FLAG47,
    48: attrs.FLAG48,
    49: attrs.FLAG49,
    50: attrs.FLAG50,
    51: attrs.FLAG51,
    52: attrs.FLAG52,
    53: attrs.FLAG53,
    54: attrs.FLAG54,
    55: attrs.FLAG55,
    56: attrs.FLAG56,
    57: attrs.FLAG57,
    58: attrs.FLAG58,
    59: attrs.FLAG59,
    60: attrs.FLAG60,
    61: attrs.FLAG61,
    62: attrs.FLAG62,
    63: attrs.FLAG63
}

POS_MAP = {
    "AUX": "AUX",
    "EOL": "EOL",
    "CCONJ": "CCONJ",
    "SCONJ": "SCONJ",
    "noun": "NOUN",
    "pronoun": "PROPN",
    "proper noun": "PROPN",
    "determiner": "DET",
    "symbol": "SYM",
    "adjective": "ADJ",
    "conjunction": "CONJ",
    "verb": "VERB",
    "pre/post-position": "ADP",
    "adverb": "ADV",
    "particle": "PART",
    "interjection": "INTJ",
    "X": "X",
    "NUM": "NUM",
    "SPACE": "SPACE",
    "PRON": "PRON"
}


FLAG_ID = 20


class SpacyRuleExtractor(Extractor):
    def __init__(self,
                 nlp,
                 rules: Dict,
                 extractor_name: str) -> None:
        """
        Initialize the extractor, storing the rule information and construct spacy rules
        Args:
            nlp
            rules: Dict
            extractor_name: str

        Returns:
        """

        Extractor.__init__(self,
                           input_type=InputType.TEXT,
                           category="spacy_rule_extractor",
                           name=extractor_name)
        self.rules = rules["rules"]
        self.nlp = copy.deepcopy(nlp)
        self.tokenizer = Tokenizer(self.nlp)
        self.matcher = Matcher(self.nlp.vocab)
        self.field_name = rules["field_name"]
        self.rule_lst = []
        self.hash_map = {}
        for a_rule in self.rules:
            this_rule = Rule(a_rule, self.nlp)
            self.rule_lst.append(this_rule)

    def extract(self, text: str) -> List[Extraction]:
        """
        Extract from text
        Args:
            text: str

        Returns: List[Extraction]
        """
        doc = self.tokenizer.tokenize_to_spacy_doc(text)
        self.load_matcher()

        matches = [x for x in self.matcher(doc) if x[1] != x[2]]
        filtered_matches = []
        for idx, start, end in matches:
            span_doc = self.tokenizer.tokenize_to_spacy_doc(doc[start:end].text)
            this_spacy_rule = self.matcher.get(idx)
            # print("===========")
            # print(this_spacy_rule)
            # print(span_doc)
            relations = self.find_relation(span_doc, this_spacy_rule)


            # rule_id, spacy_rule_id = self.hash_map[idx]

    def load_matcher(self) -> None:
        for idx, a_rule in enumerate(self.rule_lst):
            pattern_lst = [a_pattern.spacy_token_lst for a_pattern in a_rule.patterns]

            for spacy_rule_id, spacy_rule in enumerate(itertools.product(*pattern_lst)):
                self.matcher.add(self.construct_key(idx, spacy_rule_id), None, list(spacy_rule))

    """TODO: add callback function to filter custom constrains
    1. Prefix, suffix
    2. min, max
    3. full shape
    4. is in output
    5. filter overlap
    """

    def pre_suf_fix_filter(self):
        pass

    def min_max_filter(self):
        pass

    def full_shape_filter(self):
        pass

    def overlap_filter(self):
        pass

    def construct_key(self, rule_id, spacy_rule_id):
        hash_key = (rule_id, spacy_rule_id)
        hash_v = hash(hash_key)
        self.hash_map[hash_v] = hash_key
        return hash_v

    def find_relation(self, span_doc, r):
        rule = r[1][0]
        span_pivot = 0
        relation = {}
        for e_id, element in enumerate(rule):
            if not span_doc[span_pivot:]:
                for extra_id, _, in enumerate(rule[e_id:]):
                    relation[e_id+extra_id] = None
                break
            new_doc = self.tokenizer.tokenize_to_spacy_doc(span_doc[span_pivot:].text)
            if "OP" not in element:
                relation[e_id] = (span_pivot, span_pivot+1)
                span_pivot += 1
            else:
                if e_id < len(rule)-1:
                    tmp_rule_1 = [rule[e_id]]
                    tmp_rule_2 = [rule[e_id+1]]
                    tmp_matcher = Matcher(self.nlp.vocab)
                    tmp_matcher.add(0, None, tmp_rule_1)
                    tmp_matcher.add(1, None, tmp_rule_2)
                    tmp_matches = sorted([x for x in tmp_matcher(new_doc) if x[1] != x[2]], key=lambda a: a[1])

                    if not tmp_matches:
                        relation[e_id] = None
                    else:
                        matches_1 = [x for x in tmp_matches if x[0] == 0 and x[1] == 0]
                        if not matches_1:
                            relation[e_id] = None
                        else:
                            matches_2 = [x for x in tmp_matches if x[0] == 1]
                            _, s1, e1 = matches_1[0]
                            _, s2, e2 = matches_2[0]
                            if e1 <= s2:
                                relation[e_id] = (span_pivot, span_pivot + e1)
                                span_pivot += e1
                            else:
                                relation[e_id] = (span_pivot, span_pivot + s2)
                                span_pivot += s2
                else:
                    relation[e_id] = (span_pivot, len(span_doc))

        return relation


class Pattern(object):
    """
    class pattern represent each token

    For each token, we let user specify constrains for tokens. Some attributes are spacy build-in attributes,
    which can be used with rule-based matching: https://spacy.io/usage/linguistic-features#section-rule-based-matching
    Some are custom attributes, need to apply further filtering after we get matches"""
    def __init__(self, d: Dict, nlp) -> None:
        """
        Initialize a pattern, construct spacy token for matching according to type
        Args:
            d: Dict
            nlp

        Returns:
        """
        self.type = d["type"]
        self.in_output = False if d["is_in_output"] == "false" else True
        self.max = d["maximum"]
        self.min = d["minimum"]
        self.prefix = d["prefix"]
        self.suffix = d["suffix"]
        self.full_shape = d["shapes"]

        if self.type == "word":
            self.spacy_token_lst = self.construct_word_token(d, nlp)
        elif self.type == "shape":
            self.spacy_token_lst = self.construct_shape_token(d)
        elif self.type == "number":
            self.spacy_token_lst = self.construct_number_token(d, nlp)
        elif self.type == "punctuation":
            self.spacy_token_lst = self.construct_punctuation_token(d, nlp)
        elif self.type == "linebreak":
            self.spacy_token_lst = self.construct_linebreak_token(d)

    def construct_word_token(self, d: Dict, nlp) -> List[Dict]:
        """
        Construct a word token
        Args:
            d: Dict
            nlp

        Returns: List[Dict]
        """
        result = []
        if len(d["token"]) == 1:
            if d["match_all_forms"] == "true":
                this_token = {attrs.LEMMA: nlp(d["token"][0])[0].lemma_}
            else:
                this_token = {attrs.LOWER: d["token"][0].lower()}
            result.append(this_token)
            if d["capitalization"]:
                result = self.add_capitalization_constrain(result, d["capitalization"], d["token"])

        elif not d["token"]:
            if d["contain_digit"] == "true":
                this_token = {attrs.IS_ASCII: True, attrs.IS_PUNCT: False}
            else:
                this_token = {attrs.IS_ALPHA: True}
            if d["is_out_of_vocabulary"] == "true" and d["is_in_vocabulary"] != "true":
                this_token[attrs.IS_OOV] = True
            elif d["is_out_of_vocabulary"] != "true" and d["is_in_vocabulary"] == "true":
                this_token[attrs.IS_OOV] = False
            result.append(this_token)
            if d["length"]:
                result = self.add_length_constrain(result, d["length"])

        else:
            if d["match_all_forms"] == "false":
                global FLAG_ID
                token_set = set(d["token"])

                def is_selected_token(x):
                    return x in token_set

                FLAG_DICT[FLAG_ID] = nlp.vocab.add_flag(is_selected_token)
                this_token = {FLAG_DICT[FLAG_ID]: True}
                FLAG_ID += 1
                result.append(this_token)

            else:
                token_set = [nlp(x)[0].lemma_ for x in set(d["token"])]
                for a_lemma in token_set:
                    this_token = {attrs.LEMMA: a_lemma}
                    result.append(this_token)

            if d["capitalization"]:
                result = self.add_capitalization_constrain(result, d["capitalization"], d["token"])

        result = self.add_common_constrain(result, d)

        return result

    def construct_shape_token(self, d: Dict) -> List[Dict]:
        """
        Construct a shape token
        Args:
            d: Dict

        Returns: List[Dict]
        """

        result = []
        if not d["shapes"]:
            this_token = {attrs.IS_ASCII: True}
            result.append(this_token)
        else:
            for shape in d["shapes"]:
                this_shape = self.generate_shape(shape)
                this_token = {attrs.SHAPE: this_shape}
                result.append(copy.deepcopy(this_token))

        result = self.add_common_constrain(result, d)
        if d["part_of_speech"]:
            result = self.add_pos_constrain(result, d["part_of_speech"])

        return result

    def construct_number_token(self, d: Dict, nlp) -> List[Dict]:
        """
        Construct a shape token
        Args:
            d: Dict
            nlp

        Returns: List[Dict]
        """
        result = []
        if not d["numbers"]:
            this_token = {attrs.IS_DIGIT: True}
            result.append(this_token)
            if d["length"]:
                result = self.add_length_constrain(result, d["length"])
        elif len(d["numbers"]) == 1:
            this_token = {attrs.ORTH: str(d["numbers"][0])}
            result.append(this_token)
        else:
            global FLAG_ID
            number_set = set(d["numbers"])

            def is_selected_number(x):
                return x in number_set

            FLAG_DICT[FLAG_ID] = nlp.vocab.add_flag(is_selected_number)
            this_token = {FLAG_DICT[FLAG_ID]: True}
            FLAG_ID += 1
            result.append(this_token)
        result = self.add_common_constrain(result, d)
        return result

    def construct_punctuation_token(self, d: Dict, nlp) -> List[Dict]:
        """
        Construct a shape token
        Args:
            d: Dict
            nlp

        Returns: List[Dict]
        """
        result = []
        if not d["token"]:
            this_token = {attrs.IS_PUNCT: True}
        elif len(d["token"]) == 1:
            this_token = {attrs.ORTH: d["token"][0]}
        else:
            global FLAG_ID
            punct_set = set(d["token"])

            def is_selected_punct(x):
                return x in punct_set

            FLAG_DICT[FLAG_ID] = nlp.vocab.add_flag(is_selected_punct)
            this_token = {FLAG_DICT[FLAG_ID]: True}
            FLAG_ID += 1
        result.append(this_token)
        result = self.add_common_constrain(result, d)
        return result

    def construct_linebreak_token(self, d):
        """
        Construct a shape token
        Args:
            d: Dict

        Returns: List[Dict]
        """
        result = []
        num_break = int(d["length"][0]) if d["length"] else 1
        if num_break:
            s = ''
            for i in range(num_break):
                s += '\n'
            this_token = {attrs.LOWER: s}
            result.append(this_token)
            s += ' '
            this_token = {attrs.LOWER: s}
            result.append(this_token)
        result = self.add_common_constrain(result, d)

        return result

    @staticmethod
    def add_common_constrain(token_lst: List[Dict], d: Dict) -> List[Dict]:
        """
        Add common constrain for every token type, like "is_required"
        Args:
            token_lst: List[Dict]
            d: Dict

        Returns: List[Dict]
        """
        result = []
        for a_token in token_lst:
            if d["is_required"] == "false":
                a_token["OP"] = "?"
            result.append(a_token)
        return result

    @staticmethod
    def add_length_constrain(token_lst: List[Dict], lengths: List) -> List[Dict]:
        """
        Add length constrain for some token type, create cross production
        Args:
            token_lst: List[Dict]
            lengths: List

        Returns: List[Dict]
        """
        result = []
        for a_token in token_lst:
            for length in lengths:
                a_token[attrs.LENGTH] = int(length)
                result.append(copy.deepcopy(a_token))
        return result

    @staticmethod
    def add_pos_constrain(token_lst: List[Dict], pos_tags: List) -> List[Dict]:
        """
        Add pos tag constrain for some token type, create cross production
        Args:
            token_lst: List[Dict]
            pos_tags: List

        Returns: List[Dict]
        """
        result = []
        for a_token in token_lst:
            for pos in pos_tags:
                a_token[attrs.POS] = POS_MAP[pos]
                result.append(copy.deepcopy(a_token))
        return result

    @staticmethod
    def add_capitalization_constrain(token_lst: List[Dict], capi_lst: List, word_lst: List) -> List[Dict]:
        """
        Add capitalization constrain for some token type, create cross production
        Args:
            token_lst: List[Dict]
            capi_lst: List
            word_lst: List

        Returns: List[Dict]
        """
        result = []
        for a_token in token_lst:
            if "exact" in capi_lst and word_lst != []:
                for word in word_lst:
                    token = copy.deepcopy(a_token)
                    token[attrs.ORTH] = word
                    result.append(token)
            if "lower" in capi_lst:
                token = copy.deepcopy(a_token)
                token[attrs.IS_LOWER] = True
                result.append(token)
            if "upper" in capi_lst:
                token = copy.deepcopy(a_token)
                token[attrs.IS_UPPER] = True
                result.append(token)
            if "title" in capi_lst:
                token = copy.deepcopy(a_token)
                token[attrs.IS_TITLE] = True
                result.append(token)
            if "mixed" in capi_lst:
                token = copy.deepcopy(a_token)
                token[attrs.IS_UPPER] = False
                token[attrs.IS_LOWER] = False
                token[attrs.IS_TITLE] = False
                result.append(token)
        return result

    @staticmethod
    def generate_shape(word: str) -> str:
        """
        Recreate shape from a token input by user
        Args:
            word: str

        Returns: str
        """

        def counting_stars(w) -> List[int]:
            count = [1]
            for i in range(1, len(w)):
                if w[i - 1] == w[i]:
                    count[-1] += 1
                else:
                    count.append(1)

            return count

        shape = ""
        p = 0
        for c in counting_stars(word):
            if c > 4:
                shape += word[p:p + 4]
            else:
                shape += word[p:p + c]
            p = p + c

        return shape


class Rule(object):
    """
        class Rule represent each matching rule, each rule contains many pattern
    """
    def __init__(self, d: Dict, nlp) -> None:
        """
        Storing information for each Rule, create list of Pattern for a rule
        Args:
            d: Dict
            nlp

        Returns:
        """

        self.dependencies = d["dependencies"]
        self.description = d["description"]
        self.active = True if d["is_active"] == "true" else False
        self.identifier = d["identifier"]
        self.output_format = d["output_format"]
        self.polarity = False if d["polarity"] == "false" else True
        self.patterns = []
        for pattern_idx, a_pattern in enumerate(d["pattern"]):
            this_pattern = Pattern(a_pattern, nlp)
            self.patterns.append(this_pattern)

