from tokenizer import Tokenizer
import json


class ExtractableBase(object):
    """
    Encapsulates a value that can be used as an input to an Extractor
    """

    def __init__(self):
        self._value = None
        self.tokenizer = None

    @property
    def value(self):
        """
        Returns: Whatever value is returned by JSONPath
        """
        return self._value

    # def get_string(self, list_joiner: str = ", ") -> str:
    def get_string(self, list_joiner=","):
        """
        Returns: the value of the segment as a string, using a default method to convert
        objects to strings.
        """
        pass


class Extractable(ExtractableBase):
    """
    A single extraction or a single segment
    """

    def __init__(self):
        ExtractableBase.__init__(self)
        if not self.tokenizer:
            self.tokenizer = Tokenizer()
        self.tokenize_results = dict()

    def get_tokens(self, tokenizer=None):
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
        segment_value = self._value
        segment_value_str = json.dumps(segment_value, sort_keys=True)
        if (segment_value_str, tokenizer) in self.tokenize_results:
            return self.tokenize_results[(segment_value_str, tokenizer)]
        else:
            """Tokenize a string"""
            if isinstance(segment_value, str):
                tokens = self.tokenize_string(segment_value, tokenizer)
                segment_value_str = json.dumps(segment_value, sort_keys=True)
                self.tokenize_results[(segment_value_str, tokenizer)] = tokens
                return tokens
            """TODO: tokenize other segment types"""

    @staticmethod
    def tokenize_string(s, tokenizer):
        return tokenizer.tokenize(s)


class ExtractableCollection(Extractable):
    """
    A collection of PrimitveExtractable
    """

    def items(self):
        """
        Abstract method, returns a list of primitive Extractable, and should be implemented
        by each subclass.

        Returns:

        """
        pass


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
    def confidence(self):
        """
        Returns: the confidence of this extraction
        """
        return self._value["confidence"]


class ExtractionCollection(ExtractableCollection):
    """
    Encapsulates the results of an extractor, consisting or possibly multiple extractions
    """
    def __init__(self):
        ExtractableCollection.__init__(self)
        self.collection_set = set([])
        self.collection_list = list()

    def add_extraction(self, extraction):
        """
        Adds a new Extraction

        Args:
            extraction (Extraction):
        """
        extraction_str = json.dumps(extraction.value, sort_keys=True)
        if extraction_str not in self.collection_set:
            self.collection_set.add(extraction_str)
            self.collection_list.append(extraction)

    def union_extractions(self, extraction_collection):
        """
        Update this collection to include all the segments passed
        Args:
            extraction_collection: ExtractionCollection:

        Returns: self, to allow chaining
        """
        for a_extraction in extraction_collection.collection_list:
            extraction_str = json.dumps(a_extraction.value, sort_keys=True)
            if extraction_str not in self.collection_set:
                self.collection_set.add(extraction_str)
                self.collection_list.append(a_extraction)

    def all_values(self):
        """
        Convenience function.

        Returns: all values stored in all extractions in the collection
        """
        return [x.value for x in self.collection_list]

    def items(self):
        """
        Returns: all the Extraction objects as a Python list
        """
        return self.collection_list
