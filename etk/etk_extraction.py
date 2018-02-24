from spacy.tokens import Token
from etk.tokenizer import Tokenizer
from typing import List, Any


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

    def get_string(self, list_joiner: str  ="  ") -> str:
        """
        Args:
            list_joiner(str): if the value of an extractable is a list, join the elements
            using this string to separate them.

        Returns: the value of the segment as a string, using a default method to convert
        objects to strings.
        """
        if not self._value:
            return ""
        elif isinstance(self._value, list):
            return self.list2str(self._value, list_joiner)
        elif isinstance(self._value, dict):
            return self.dict2str(self._value, list_joiner)
        else:
            return str(self._value)

    def list2str(self, l: list, joiner: str) -> str:
        """
        to-do: add documentation of why this method exists

        Args:
            l ():
            joiner ():

        Returns:

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

    def dict2str(self, d: dict, joiner: str) -> str:
        """
        to-do: add documentation of why this method exists

        Args:
            d ():
            joiner ():

        Returns:

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

    def get_tokens(self, tokenizer: Tokenizer) -> List[Token]:
        """
        Tokenize this Extractable.

        If the value is a string, it returns the tokenized version of the string. Else, convert to string with
        get_string method

        As it is common to need the same tokens for multiple extractors, the Extractable should cache the
        tokenization results, keyed by segment and tokenizer so that given the same segment and tokenizer,
        the same results are returned. If the same segment is given, but different tokenizer, the different
        results are cached separately.

        Args:
            extractable (Extractable): any part of a JSON document
            tokenizer (Tokenizer)
            the document will be used.

        Returns: a sequence of tokens.
        """

        if (self, tokenizer) in self.tokenize_results:
            return self.tokenize_results[(self, tokenizer)]
        else:
            segment_value_for_tokenize = self.get_string()
            tokens = tokenizer.tokenize(segment_value_for_tokenize)
            self.tokenize_results[(self, tokenizer)] = tokens
            return tokens


class Extraction(Extractable):
    """
    Encapsulates the results of an extractor.
    Note that Extractions are Extractable, so they can be used as inputs to other extractors.
    """

    def __init__(self, extracted_result: object):
        Extractable.__init__(self)
        """

        Args:
            extracted_result (dict): the extracted result should be dict containing information like.
                value, extractor_name, confidence, start_token, end_token, start_char, end_char

        Returns:

        """
        fake_extraction = {"extracted_value": extracted_result["value"], "confidence": extracted_result["confidence"]}
        # pseudo-code below
        # self.provenance = Provenance(extractor_name=extractor_name, confidence=confidence, start_token=start_token, end_token=end_token,
        #                   start_char=start_char, end_char=end_char)
        # prov_id = document.add_provenance(self.provenance)
        # self._value = ExtractionValue(value, prov_id)
        self._value = fake_extraction

    @property
    def value(self) -> object:
        """
        Returns: the value produced by an extractor
        """
        return self._value

    @property
    def confidence(self) -> float:
        """
        Returns: the confidence of this extraction
        """
        return self._value["confidence"]
