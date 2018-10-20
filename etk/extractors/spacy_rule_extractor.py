from typing import List, Dict
from etk.extractor import Extractor, InputType
from etk.extraction import Extraction
from etk.tokenizer import Tokenizer
from spacy.matcher import Matcher
from spacy import attrs
from spacy.tokens import span, doc
from etk.extractors.util.util import tf_transfer
import copy
import itertools
import sys
import re

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
    """
    **Description**
        This extractor takes a spaCy rule as reference and extracts the substring
        which matches the given spaCy rule.

    Examples:
        ::

            rules = json.load(open('path_to_spacy_rules.json', "r"))
            sample_rules = rules["test_SpacyRuleExtractor_word_1"]
            spacy_rule_extractor = SpacyRuleExtractor(nlp=nlp,
                                                     rules=sample_rules)
            spacy_rule_extractor.extract(text=text)

    """

    def __init__(self,
                 nlp,
                 rules: Dict,
                 extractor_name: str) -> None:
        """
        Initialize the extractor, storing the rule information and construct spacy rules
        Args:
            nlp
            rules (Dict): spacy rules
            extractor_name: str

        Returns:
        """

        Extractor.__init__(self,
                           input_type=InputType.TEXT,
                           category="spacy_rule_extractor",
                           name=extractor_name)
        self._rules = rules["rules"]
        self._nlp = copy.deepcopy(nlp)
        self._tokenizer = Tokenizer(self._nlp)
        self._matcher = Matcher(self._nlp.vocab)
        self._field_name = rules["field_name"] if "field_name" in rules else extractor_name
        self._rule_lst = {}
        self._hash_map = {}
        for idx, a_rule in enumerate(self._rules):
            this_rule = Rule(a_rule, self._nlp)
            self._rule_lst[this_rule.identifier + "rule_id##" + str(idx)] = this_rule

    def extract(self, text: str) -> List[Extraction]:
        """
        Extract from text

        Args:
            text (str): input str to be extracted.

        Returns:
            List[Extraction]: the list of extraction or the empty list if there are no matches.
        """

        doc = self._tokenizer.tokenize_to_spacy_doc(text)
        self._load_matcher()

        matches = [x for x in self._matcher(doc) if x[1] != x[2]]
        pos_filtered_matches = []
        neg_filtered_matches = []
        for idx, start, end in matches:
            span_doc = self._tokenizer.tokenize_to_spacy_doc(doc[start:end].text)
            this_spacy_rule = self._matcher.get(idx)
            relations = self._find_relation(span_doc, this_spacy_rule)
            rule_id, _ = self._hash_map[idx]
            this_rule = self._rule_lst[rule_id]
            if self._filter_match(doc[start:end], relations, this_rule.patterns):
                value = self._form_output(doc[start:end], this_rule.output_format, relations, this_rule.patterns)
                if this_rule.polarity:
                    pos_filtered_matches.append((start, end, value, rule_id, relations))
                else:
                    neg_filtered_matches.append((start, end, value, rule_id, relations))

        return_lst = []
        if pos_filtered_matches:
            longest_lst_pos = self._get_longest(pos_filtered_matches)
            if neg_filtered_matches:
                longest_lst_neg = self._get_longest(neg_filtered_matches)
                return_lst = self._reject_neg(longest_lst_pos, longest_lst_neg)
            else:
                return_lst = longest_lst_pos

        extractions = []
        for (start, end, value, rule_id, relation) in return_lst:
            this_extraction = Extraction(value=value,
                                         extractor_name=self.name,
                                         start_token=start,
                                         end_token=end,
                                         start_char=doc[start].idx,
                                         end_char=doc[end-1].idx+len(doc[end-1]),
                                         rule_id=rule_id.split("rule_id##")[0],
                                         match_mapping=relation)
            extractions.append(this_extraction)

        return extractions

    def _load_matcher(self) -> None:
        """
        Add constructed spacy rule to Matcher
        """
        for id_key in self._rule_lst:
            if self._rule_lst[id_key].active:
                pattern_lst = [a_pattern.spacy_token_lst for a_pattern in self._rule_lst[id_key].patterns]

                for spacy_rule_id, spacy_rule in enumerate(itertools.product(*pattern_lst)):
                    self._matcher.add(self._construct_key(id_key, spacy_rule_id), None, list(spacy_rule))

    def _filter_match(self, span: span, relations: Dict, patterns: List) -> bool:
        """
        Filter the match result according to prefix, suffix, min, max ...
        Args:
            span: span
            relations: Dict
            patterns: List of pattern

        Returns: bool
        """

        for pattern_id, a_pattern in enumerate(patterns):
            token_range = relations[pattern_id]
            if token_range:
                tokens = [x for x in span[token_range[0]:token_range[1]]]
                if a_pattern.type == "word":
                    if not self._pre_suf_fix_filter(tokens, a_pattern.prefix, a_pattern.suffix):
                        return False
                if a_pattern.type == "shape":
                    if not (self._full_shape_filter(tokens, a_pattern.full_shape)
                            and self._pre_suf_fix_filter(tokens, a_pattern.prefix,a_pattern.suffix)):
                        return False
                if a_pattern.type == "number":
                    if not self._min_max_filter(tokens, a_pattern.min, a_pattern.max):
                        return False
        return True

    @staticmethod
    def _get_longest(value_lst: List) -> List:
        """
        Get the longest match for overlap
        Args:
            value_lst: List

        Returns: List
        """

        value_lst.sort()
        result = []
        pivot = value_lst[0]
        start, end = pivot[0], pivot[1]
        pivot_e = end
        pivot_s = start
        for idx, (s, e, v, rule_id, _) in enumerate(value_lst):
            if s == pivot_s and pivot_e < e:
                pivot_e = e
                pivot = value_lst[idx]
            elif s != pivot_s and pivot_e < e:
                result.append(pivot)
                pivot = value_lst[idx]
                pivot_e = e
                pivot_s = s
        result.append(pivot)
        return result

    @staticmethod
    def _reject_neg(pos_lst: List, neg_lst: List) -> List:
        """
        Reject some positive matches according to negative matches
        Args:
            pos_lst: List
            neg_lst: List

        Returns: List
        """

        pos_lst.sort()
        neg_lst.sort()
        result = []
        pivot_pos = pos_lst[0]
        pivot_neg = neg_lst[0]
        while pos_lst:
            if pivot_pos[1] <= pivot_neg[0]:
                result.append(pivot_pos)
                pos_lst.pop(0)
                if pos_lst:
                    pivot_pos = pos_lst[0]
            elif pivot_pos[0] >= pivot_neg[1]:
                neg_lst.pop(0)
                if not neg_lst:
                    result += pos_lst
                    break
                else:
                    pivot_neg = neg_lst[0]
            else:
                pos_lst.pop(0)
                if pos_lst:
                    pivot_pos = pos_lst[0]
        return result

    @staticmethod
    def _pre_suf_fix_filter(t: List, prefix: str, suffix: str) -> bool:
        """
        Prefix and Suffix filter
        Args:
            t: List, list of tokens
            prefix: str
            suffix: str

        Returns: bool
        """

        if prefix:
            for a_token in t:
                if a_token._.n_prefix(len(prefix)) != prefix:
                    return False
        if suffix:
            for a_token in t:
                if a_token._.n_suffix(len(suffix)) != suffix:
                    return False

        return True

    @staticmethod
    def _min_max_filter(t: List, min_v: str, max_v: str) -> bool:
        """
        Min and Max filter
        Args:
            t: List, list of tokens
            min_v: str
            max_v: str

        Returns: bool
        """

        def tofloat(value):
            try:
                float(value)
                return float(value)
            except ValueError:
                return False

        for a_token in t:
            if not tofloat(a_token.text):
                return False
            else:
                if min_v and tofloat(min_v):
                    this_v = tofloat(a_token.text)
                    if this_v < tofloat(min_v):
                        return False
                if max_v and tofloat(max_v):
                    this_v = tofloat(a_token.text)
                    if this_v > tofloat(max_v):
                        return False

        return True

    @staticmethod
    def _full_shape_filter(t: List, shapes: List) -> bool:
        """
        Shape filter
        Args:
            t: List, list of tokens
            shapes: List

        Returns: bool
        """

        if shapes:
            for a_token in t:
                if a_token._.full_shape not in shapes:
                    return False

        return True

    @staticmethod
    def _form_output(span_doc: span, output_format: str, relations: Dict, patterns: List) -> str:
        """
        Form an output value according to user input of output_format
        Args:
            span_doc: span
            format: str
            relations: Dict
            patterns: List

        Returns: str
        """

        format_value = []
        output_inf = [a_pattern.in_output for a_pattern in patterns]
        for i in range(len(output_inf)):
            token_range = relations[i]
            if token_range and output_inf[i]:
                format_value.append(span_doc[token_range[0]:token_range[1]].text)

        if not output_format:
            return " ".join(format_value)

        result_str = re.sub("{}", " ".join(format_value), output_format)

        positions = re.findall("{[0-9]+}", result_str)

        if not positions:
            return result_str

        position_indices = [int(x[1:-1]) for x in positions]
        if max(position_indices) < len(format_value):
            result_str = result_str.format(*format_value)
        else:
            try:
                result_str = result_str.format("", *format_value)
            except:
                positions = [x for x in positions if int(x[1:-1]) > len(format_value)-1 or int(x[1:-1]) < 0]
                for pos in positions:
                    result_str = result_str.replace(pos, "")
                result_str = result_str.format(*format_value)

        return result_str

    def _construct_key(self, rule_id: str, spacy_rule_id:int) -> int:
        """
        Use a mapping to store the information about rule_id for each matches, create the mapping key here
        Args:
            rule_id: str
            spacy_rule_id:int

        Returns: int
        """

        hash_key = (rule_id, spacy_rule_id)
        hash_v = hash(hash_key) + sys.maxsize + 1
        self._hash_map[hash_v] = hash_key
        return hash_v

    def _find_relation(self, span_doc: doc, r: List) -> Dict:
        """
        Get the relations between the each pattern in the spacy rule and the matches
        Args:
            span_doc: doc
            r: List

        Returns: Dict
        """

        rule = r[1][0]
        span_pivot = 0
        relation = {}
        for e_id, element in enumerate(rule):
            if not span_doc[span_pivot:]:
                for extra_id, _, in enumerate(rule[e_id:]):
                    relation[e_id+extra_id] = None
                break
            new_doc = self._tokenizer.tokenize_to_spacy_doc(span_doc[span_pivot:].text)
            if "OP" not in element:
                relation[e_id] = (span_pivot, span_pivot+1)
                span_pivot += 1
            else:
                if e_id < len(rule)-1:
                    tmp_rule_1 = [rule[e_id]]
                    tmp_rule_2 = [rule[e_id+1]]
                    tmp_matcher = Matcher(self._nlp.vocab)
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
                            _, s1, e1 = matches_1[0]
                            matches_2 = [x for x in tmp_matches if x[0] == 1]
                            if not matches_2:
                                relation[e_id] = (span_pivot, span_pivot + e1)
                                span_pivot += e1
                            else:
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
    Some are custom attributes, need to apply further filtering after we get matches
    """

    def __init__(self, d: Dict, nlp) -> None:
        """
        Initialize a pattern, construct spacy token for matching according to type
        Args:
            d: Dict
            nlp

        Returns:
        """

        self.type = d["type"]
        self.in_output = tf_transfer(d["is_in_output"])
        self.max = d["maximum"]
        self.min = d["minimum"]
        self.prefix = d["prefix"]
        self.suffix = d["suffix"]
        self.full_shape = d.get("shapes")

        if self.type == "word":
            self.spacy_token_lst = self._construct_word_token(d, nlp)
        elif self.type == "shape":
            self.spacy_token_lst = self._construct_shape_token(d)
        elif self.type == "number":
            self.spacy_token_lst = self._construct_number_token(d, nlp)
        elif self.type == "punctuation":
            self.spacy_token_lst = self._construct_punctuation_token(d, nlp)
        elif self.type == "linebreak":
            self.spacy_token_lst = self._construct_linebreak_token(d)

    def _construct_word_token(self, d: Dict, nlp) -> List[Dict]:
        """
        Construct a word token
        Args:
            d: Dict
            nlp

        Returns: List[Dict]
        """

        result = []
        if len(d["token"]) == 1:
            if tf_transfer(d["match_all_forms"]):
                this_token = {attrs.LEMMA: nlp(d["token"][0])[0].lemma_}
            else:
                this_token = {attrs.LOWER: d["token"][0].lower()}
            result.append(this_token)
            if d["capitalization"]:
                result = self._add_capitalization_constrain(result, d["capitalization"], d["token"])

        elif not d["token"]:
            if tf_transfer(d["contain_digit"]):
                this_token = {attrs.IS_ASCII: True, attrs.IS_PUNCT: False}
            else:
                this_token = {attrs.IS_ALPHA: True}
            if tf_transfer(d["is_out_of_vocabulary"]) and not tf_transfer(d["is_in_vocabulary"]):
                this_token[attrs.IS_OOV] = True
            elif not tf_transfer(d["is_out_of_vocabulary"]) and tf_transfer(d["is_in_vocabulary"]):
                this_token[attrs.IS_OOV] = False
            result.append(this_token)
            if d["length"]:
                result = self._add_length_constrain(result, d["length"])
            if d["capitalization"]:
                result = self._add_capitalization_constrain(result, d["capitalization"], d["token"])

        else:
            if "match_all_forms" in d and not tf_transfer(d["match_all_forms"]):
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
                result = self._add_capitalization_constrain(result, d["capitalization"], d["token"])

        result = self._add_common_constrain(result, d)
        if d["part_of_speech"]:
            result = self._add_pos_constrain(result, d["part_of_speech"])

        return result

    def _construct_shape_token(self, d: Dict) -> List[Dict]:
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
                this_shape = self._generate_shape(shape)
                this_token = {attrs.SHAPE: this_shape}
                result.append(copy.deepcopy(this_token))

        result = self._add_common_constrain(result, d)
        if d["part_of_speech"]:
            result = self._add_pos_constrain(result, d["part_of_speech"])

        return result

    def _construct_number_token(self, d: Dict, nlp) -> List[Dict]:
        """
        Construct a shape token
        Args:
            d: Dict
            nlp

        Returns: List[Dict]
        """

        result = []
        if not d["numbers"]:
            this_token = {attrs.LIKE_NUM: True}
            result.append(this_token)
            if d["length"]:
                result = self._add_length_constrain(result, d["length"])
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
        result = self._add_common_constrain(result, d)
        return result

    def _construct_punctuation_token(self, d: Dict, nlp) -> List[Dict]:
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
        result = self._add_common_constrain(result, d)
        return result

    def _construct_linebreak_token(self, d: Dict) -> List[Dict]:
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
        result = self._add_common_constrain(result, d)

        return result

    @staticmethod
    def _add_common_constrain(token_lst: List[Dict], d: Dict) -> List[Dict]:
        """
        Add common constrain for every token type, like "is_required"
        Args:
            token_lst: List[Dict]
            d: Dict

        Returns: List[Dict]
        """

        result = []
        for a_token in token_lst:
            if not tf_transfer(d["is_required"]):
                a_token["OP"] = "?"
            result.append(a_token)
        return result

    @staticmethod
    def _add_length_constrain(token_lst: List[Dict], lengths: List) -> List[Dict]:
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
                if type(length) == str and length and length.isdigit():
                    a_token[attrs.LENGTH] = int(length)
                    result.append(copy.deepcopy(a_token))
                elif type(length) == int:
                    a_token[attrs.LENGTH] = int(length)
                    result.append(copy.deepcopy(a_token))
        return result

    @staticmethod
    def _add_pos_constrain(token_lst: List[Dict], pos_tags: List) -> List[Dict]:
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
    def _add_capitalization_constrain(token_lst: List[Dict], capi_lst: List, word_lst: List) -> List[Dict]:
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
    def _generate_shape(word: str) -> str:
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
        Class Rule represent each matching rule, each rule contains many pattern
    """

    def __init__(self, d: Dict, nlp) -> None:
        """
        Storing information for each Rule, create list of Pattern for a rule
        Args:
            d: Dict
            nlp

        Returns:
        """

        self.dependencies = d["dependencies"] if "dependencies" in d else []
        self.description = d["description"] if "description" in d else ""
        self.active = tf_transfer(d["is_active"])
        self.identifier = d["identifier"]
        self.output_format = d["output_format"]
        self.polarity = tf_transfer(d["polarity"])
        self.patterns = []
        for pattern_idx, a_pattern in enumerate(d["pattern"]):
            this_pattern = Pattern(a_pattern, nlp)
            self.patterns.append(this_pattern)

