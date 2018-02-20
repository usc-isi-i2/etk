import json
from tokenizer import Tokenizer


class Document(object):
    """
        This class wraps raw CDR documents and provides a convenient API for ETK
        to query elements of the document and to update the document with the results
        of extractors.
        """

    def __init__(self, cdr_document, tokenizer=None):
        """
        Wrapper object for CDR documents.

        Args:
            cdr_document (JSON): the raw CDR document received in ETK.
            tokenizer (Tokenizer): the default tokenizer for creating tokens.

        Returns: the wrapped CDR document

        """
        self.cdr_document = json.loads(cdr_document)
        self.tokenization_results = dict()
        if tokenizer:
            self.tokenizer = tokenizer
        else:
            self.tokenizer = Tokenizer(keep_multi_space=True)

    def select_containers(self, json_path):
        """
        Dereferences the json_path inside the document and returns the selected elements.
        This method should compile and cache the compiled json_path in case the same path
        is reused by multiple extractors.

        Args:
            json_path (String): a valid JSON path specification.


        Returns: the results of applying the json_path on the document if exist else None.

        Eg.
        1. "./__content_strict"   key "__content_strict" under root
        2. "."   ROOT
        """
        pass

    def get_tokens(self, segment, tokenizer=None):
        """
        Tokenizes the given segment.

        1. If the segment is a string, it returns the tokenized version of the string.
        2. If the segment is a List, it recursively tokenizes each element of the list.
        3. If the segment is a dict, it recursively tokenizes the value of each key/value pair, inserting a
        newline token after each key/value pair. The order should be by lexicographic order of the keys so that
        tokenizationis repeatable.

        As it is common to need the same tokens for multiple extractors, the Document should cache the
        tokenization results, keyed by segment and tokenizer so that given the same segment and tokenizer,
        the same results are returned. If the same segment is given, but different tokenizer, the different
        results are cached separately.

        Args:
            segment (JSON): any part of a JSON document
            tokenizer (Tokenizer): used if provided, otherwise the default tokenizer of the document will be used.

        Returns: a seqeuence of tokens.
        """
        if not tokenizer:
            tokenizer = self.tokenizer
        if (segment, tokenizer) in self.tokenization_results:
            return self.tokenization_results[(segment, tokenizer)]
        else:
            """Tokenize a string"""
            if isinstance(segment, str):
                tokens = self.tokenize_string(segment, tokenizer)
                self.tokenization_results[(segment, tokenizer)] = tokens
                return tokens
            """TODO: tokenize other segment types"""

    def store_extraction(self, extractor, extraction, container, output_key):
        """

        Args:
            extractor():
            extraction ():
            container ():
            output_key ():

        Returns:

        """
        container[output_key] = extraction

    @staticmethod
    def tokenize_string(s, tokenizer):
        return tokenizer.tokenize(s)
