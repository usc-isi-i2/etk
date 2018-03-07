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
        for a_rule in self.rules:
            this_rule = Rule(a_rule, self.nlp)
            self.rule_lst.append(this_rule)

    def extract(self, text: str) -> List[Extraction]:
        doc = self.tokenizer.tokenize_to_spacy_doc(text)
        # for i in doc:
        #     print(i.orth_)
        #     print(i.pos_)
        #     print(i.shape_)
        self.load_matcher()
        """TODO: add callback function to filter custom constrains"""
        for idx, start, end in self.matcher(doc):
            print(idx, doc[start:end])

    def load_matcher(self) -> None:
        for idx, a_rule in enumerate(self.rule_lst):
            pattern_flat_lst = [a_pattern.spacy_token_lst for a_pattern in a_rule.patterns]
            for element in itertools.product(*pattern_flat_lst):
                x = list(element)
                print(x)
                self.matcher.add(idx, None, x)


class Pattern(object):
    """For each token, we let user specify constrains for tokens. Some attributes are spacy build-in attributes,
    which can be used with rule-based matching: https://spacy.io/usage/linguistic-features#section-rule-based-matching
    Some are custom attributes, need to apply further filtering after we get matches"""
    def __init__(self, d: Dict, nlp) -> None:
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

        return result

    def construct_shape_token(self, d: Dict) -> List[Dict]:
        result = []
        if not d["shapes"]:
            this_token = {attrs.IS_ASCII: True}
            result.append(this_token)
        else:
            for shape in d["shapes"]:
                this_shape = self.generate_shape(shape, self.counting_stars(shape))
                this_token = {attrs.SHAPE: this_shape}
                result.append(copy.deepcopy(this_token))

        result = self.add_common_constrain(result, d)
        if d["part_of_speech"]:
            result = self.add_pos_constrain(result, d["part_of_speech"])

        return result

    def construct_number_token(self, d: Dict, nlp) -> List[Dict]:
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
        result = []
        num_break = int(d["length"][0]) if d["length"] else 1
        if num_break:
            s = ''
            for i in range(num_break):
                s += '\n'
            s += ' '
            this_token = {attrs.LOWER: s}
            result.append(this_token)
        result = self.add_common_constrain(result, d)

        return result

    @staticmethod
    def add_common_constrain(token_lst: List[Dict], d: Dict) -> List[Dict]:
        result = []
        for a_token in token_lst:
            if d["is_required"] == "false":
                a_token["OP"] = "?"
            result.append(a_token)
        return result

    @staticmethod
    def add_length_constrain(token_lst: List[Dict], lengths: List) -> List[Dict]:
        result = []
        for a_token in token_lst:
            for length in lengths:
                a_token[attrs.LENGTH] = int(length)
                result.append(copy.deepcopy(a_token))
        return result

    @staticmethod
    def add_pos_constrain(token_lst: List[Dict], pos_tags: List) -> List[Dict]:
        result = []
        for a_token in token_lst:
            for pos in pos_tags:
                a_token[attrs.POS] = POS_MAP[pos]
                result.append(copy.deepcopy(a_token))
        return result

    @staticmethod
    def add_capitalization_constrain(token_lst: List[Dict], capi_lst: List, word_lst: List) -> List[Dict]:
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
    def generate_shape(word: str, count: List) -> str:
        shape = ""
        p = 0
        for c in count:
            if c > 4:
                shape += word[p:p + 4]
            else:
                shape += word[p:p + c]
            p = p + c

        return shape

    @staticmethod
    def counting_stars(word: str) -> List[int]:
        count = [1]
        for i in range(1, len(word)):
            if word[i - 1] == word[i]:
                count[-1] += 1
            else:
                count.append(1)

        return count


class Rule(object):
    def __init__(self, d: Dict, nlp) -> None:
        self.dependencies = d["dependencies"]
        self.description = d["description"]
        self.active = True if d["is_active"] == "true" else False
        self.identifier = d["identifier"]
        self.output_format = d["output_format"]
        self.polarity = False if d["polarity"] == "false" else True
        self.patterns = []
        for a_pattern in d["pattern"]:
            this_pattern = Pattern(a_pattern, nlp)
            self.patterns.append(this_pattern)

