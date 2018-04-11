from spacy.tokens import Token
from etk.tokenizer import Tokenizer
from typing import List, Any, Dict


class ExtractableBase(object):
    """
    Encapsulates a value that can be used as an input to an Extractor
    """

    def __init__(self) -> None:
        self._value = None

    @property
    def value(self) -> Any:
        """
        Returns: Whatever value is returned by JSONPath
        """
        return self._value

    def get_string(self, joiner: str ="  ") -> str:
        """
        Args:
            joiner(str): if the value of an extractable is not a string, join the elements
            using this string to separate them.

        Returns: the value of the segment as a string, using a default method to convert
        objects to strings.
        """
        if not self._value:
            return ""
        elif isinstance(self._value, list):
            return self.list2str(self._value, joiner)
        elif isinstance(self._value, dict):
            return self.dict2str(self._value, joiner)
        else:
            return str(self._value)

    def list2str(self, l: List, joiner: str) -> str:
        """
        Convert list to str as input for tokenizer

        Args:
            l (list): list for converting
            joiner (str): join the elements using this string to separate them.

        Returns: the value of the list as a string

        """
        result = str()
        for item in l:
            if isinstance(item, list):
                result = result + self.list2str(item, joiner) + joiner
            elif isinstance(item, dict):
                result = result + self.dict2str(item, joiner) + joiner
            elif item:
                result = result + str(item) + joiner
        return result

    def dict2str(self, d: Dict, joiner: str) -> str:
        """
        Convert dict to str as input for tokenizer

        Args:
            d (dict): dict for converting
            joiner (str): join the elements using this string to separate them.

        Returns: the value of the dict as a string

        """
        result = str()
        for key in d:
            result = result + str(key) + " : "
            if isinstance(d[key], list):
                result = result + self.list2str(d[key], joiner) + joiner
            elif isinstance(d[key], dict):
                result = result + self.dict2str(d[key], joiner) + joiner
            elif d[key]:
                result = result + str(d[key]) + joiner
        return result


class Extractable(ExtractableBase):
    """
    A single extraction or a single segment
    """

    def __init__(self, value=None) -> None:
        ExtractableBase.__init__(self)
        self.tokenize_results = dict()
        self._value = value

    def get_tokens(self, tokenizer: Tokenizer, keep_multi_space: bool = False) -> List[Token]:
        """
        Tokenize this Extractable.

        If the value is a string, it returns the tokenized version of the string. Else, convert to string with
        get_string method

        As it is common to need the same tokens for multiple extractors, the Extractable should cache the
        tokenization results, keyed by segment and tokenizer so that given the same segment and tokenizer,
        the same results are returned. If the same segment is given, but different tokenizer, the different
        results are cached separately.

        Args:
            tokenizer (Tokenizer)
            keep_multi_space

        Returns: a sequence of tokens.
        """

        if (self, tokenizer) in self.tokenize_results:
            return self.tokenize_results[(self, tokenizer)]
        else:
            if isinstance(self._value, list):
                print("\n========tokenizer needs string, got list, converting list to string========")
            elif isinstance(self._value, dict):
                print("\n========tokenizer needs string, got dict, converting dict to string========")
            segment_value_for_tokenize = self.get_string()
            tokens = tokenizer.tokenize(segment_value_for_tokenize, keep_multi_space)
            self.tokenize_results[(self, tokenizer)] = tokens
            return tokens


class Extraction(Extractable):
    """
    Encapsulates the results of an extractor.
    Note that Extractions are Extractable, so they can be used as inputs to other extractors.
    """

    def __init__(self,
                 value,
                 extractor_name: str,
                 confidence=1.0,
                 start_token=None,
                 end_token=None,
                 start_char=None,
                 end_char=None,
                 **options):
        Extractable.__init__(self)
        """

        Args:
            extracted_result (dict): the extracted result should be dict containing information like.
                value, extractor_name, confidence, start_token, end_token, start_char, end_char

        Returns:

        """
        self._addition_inf = dict()
        self._addition_inf["tag"] = options["tag"] if "tag" in options else None
        self._addition_inf["spacy_rule_id"] = options["rule_id"] if "rule_id" in options else None
        self._addition_inf["spacy_rule_mapping"] = options["match_mapping"] if "match_mapping" in options else None
        self._addition_inf["date_object"] = options["date_object"] if "date_object" in options else None
        self._addition_inf["original_date"] = options["original_date"] if "original_date" in options else None
        self._extractor_name = extractor_name
        self._offsets = {
            "start_token": start_token,
            "end_token": end_token,
            "start_char": start_char,
            "end_char": end_char
        }
        self._confidence = confidence

        # pseudo-code below
        # self.provenance = Provenance(extractor_name=extractor_name, confidence=confidence, start_token=start_token, end_token=end_token,
        #                   start_char=start_char, end_char=end_char)
        # prov_id = document.add_provenance(self.provenance)
        # self._value = ExtractionValue(value, prov_id)
        self._value = value

    def __str__(self):
        return str(self.__class__) + ": " + str(self.__dict__)

    @property
    def value(self) -> Dict or str:
        """
        Returns: the value produced by an extractor
        """
        return self._value

    @property
    def confidence(self) -> float:
        """
        Returns: the confidence of this extraction
        """
        return self._confidence

    @property
    def offsets(self) -> Dict:
        """
        Returns: the offset inf of this extraction
        """
        return self._offsets

    @property
    def name(self) -> str:
        """
        Returns: the name of this extraction
        """
        return self._extractor_name

    @property
    def tag(self) -> str:
        """

        Returns: the tag associated with this Extraction.

        """
        return self._addition_inf["tag"]

    @property
    def rule_id(self) -> str:
        """

        Returns: the rule_id associated with this Extraction.

        """
        return self._addition_inf["spacy_rule_id"]

    @property
    def spacy_rule_mapping(self) -> Dict:
        """

        Returns: the spacy_rule_mapping associated with this Extraction.

        """
        return self._addition_inf["spacy_rule_mapping"]

    @property
    def original_date(self):
        """

        Returns: the original_date associated with this Extraction.

        """
        return self._addition_inf["original_date"]

    @property
    def date_object(self):
        """

        Returns: the original_date associated with this Extraction.

        """
        return self._addition_inf["date_object"]
