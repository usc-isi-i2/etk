from etk2.tokenizer import Tokenizer
from spacy.tokens import Token
from typing import List, Any

class ExtractableBase(object):
    """
    Encapsulates a value that can be used as an input to an Extractor
    """

    def __init__(self) -> None:
        self._value = None
        self.tokenizer = None

    @property
    def value(self) -> Any:
        """
        Returns: Whatever value is returned by JSONPath
        """
        return self._value

    # def get_string(self, list_joiner: str = ", ") -> str:
    def get_string(self, list_joiner="  ") -> str:
        """
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

    def list2str(self, l, joiner) -> str:
        result = str()
        for item in l:
            if isinstance(item, list):
                result = result + self.list2str(item, joiner) + joiner
            elif isinstance(item, dict):
                result = result + self.dict2str(item, joiner) + joiner
            elif not item:
                result = result + ""
            else:
                result = result + str(item) + joiner
        return result

    def dict2str(self, d, joiner) -> str:
        result = str()
        for key in d:
            result = result + str(key) + " : "
            if isinstance(d[key], list):
                result = result + self.list2str(d[key], joiner) + joiner
            elif isinstance(d[key], dict):
                result = result + self.dict2str(d[key], joiner) + joiner
            elif not d[key]:
                result = result + ""
            else:
                result = result + str(d[key]) + joiner
        return result


class Extractable(ExtractableBase):
    """
    A single extraction or a single segment
    """

    def __init__(self, value=None, nlp=None) -> None:
        ExtractableBase.__init__(self)
        if not self.tokenizer:
            self.tokenizer = Tokenizer(nlp)
        self.tokenize_results = dict()
        self._value = value

    def get_tokens(self, tokenizer=None) -> List[Token]:
        """
        Tokenize this Extractable.

        If the value is a string, it returns the tokenized version of the string. If the value
        is a List, it recursively tokenizes each element of the list. If the value is a dict, it
        recursively tokenizes the value of each key/value pair, inserting a newline token after each
        key/value pair. The order should be by lexicographic order of the keys so that tokenization
        is repeatable.

        As it is common to need the same tokens for multiple extractors, the Extractable should cache the
        tokenization results, keyed by segment and tokenizer so that given the same segment and tokenizer,
        the same results are returned. If the same segment is given, but different tokenizer, the different
        results are cached separately.

        Args:
            extractable (Extractable): any part of a JSON document
            tokenizer (Tokenizer): used if provided, otherwise the default tokenizer of
            the document will be used.

        Returns: a sequence of tokens.
        """

        if not tokenizer:
            tokenizer = self.tokenizer
        if (self, tokenizer) in self.tokenize_results:
            return self.tokenize_results[(self, tokenizer)]
        else:
            segment_value_for_tokenize = self.get_string()
            tokens = self.tokenize_string(segment_value_for_tokenize, tokenizer)
            self.tokenize_results[(self, tokenizer)] = tokens
            return tokens

    @staticmethod
    def tokenize_string(s, tokenizer) -> List[Token]:
        return tokenizer.tokenize(s)


class ExtractableCollection(object):
    """
    A collection of PrimitveExtractable
    """
    def __init__(self) -> None:
        self.collection_set = set([])
        self.collection_list = list()

    def items(self) -> List:
        """
        Returns a list of primitive item in collection, and should be implemented
        by each subclass.

        Returns:

        """
        return self.collection_list

    def all_values(self) -> List:
        """
        Convenience function.

        Returns: list of all values stored in all item in the collection
        """
        return [x.value for x in self.collection_list]


class Extraction(Extractable):
    """
    Encapsulates the results of an extractor.
    Note that Extractions are Extractable, so they can be used as inputs to other extractors.
    """

    def __init__(self,
                 value,
                 extractor_name=None,
                 confidence=None,
                 start_token=None,
                 end_token=None,
                 start_char=None,
                 end_char=None):
        Extractable.__init__(self)
        """

        Args:
            value (object): the extracted value can be any JSON serializable Python object.
            extractor_name (str):
            confidence (float):
            start_token (int):
            end_token (int):
            start_char (int):
            end_char (int):

        Returns:

        """
        fake_extraction = {"extracted_value": value, "confidence": confidence}
        # pseudo-code below
        # self.provenance = Provenance(extractor_name=extractor_name, confidence=confidence, start_token=start_token, end_token=end_token,
        #                   start_char=start_char, end_char=end_char)
        # prov_id = document.add_provenance(self.provenance)
        # self._value = ExtractionValue(value, prov_id)
        self._value = fake_extraction

    @property
    def value(self):
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


class ExtractionCollection(ExtractableCollection):
    """
    Encapsulates the results of an extractor, consisting or possibly multiple extractions
    """
    def __init__(self) -> None:
        ExtractableCollection.__init__(self)

    def add_extraction(self, extraction) -> None:
        """
        Adds a new Extraction

        Args:
            extraction (Extraction):
        """
        if extraction not in self.collection_set:
            self.collection_set.add(extraction)
            self.collection_list.append(extraction)

    def union_extractions(self, extraction_collection) -> None:
        """
        Update this collection to include all the segments passed
        Args:
            extraction_collection: ExtractionCollection:

        Returns: self, to allow chaining
        """
        for a_extraction in extraction_collection.collection_list:
            if a_extraction not in self.collection_set:
                self.collection_set.add(a_extraction)
                self.collection_list.append(a_extraction)
