from warnings import warn
from typing import List
from etk.extractor import Extractor, InputType
from etk.extraction import Extraction
from etk.tokenizer import Tokenizer
from etk.etk_exceptions import ExtractorError
from spacy.tokens import Token
from pygtrie import CharTrie
from itertools import *
from functools import reduce
import re

disable_spacy = ['tagger', 'parser', 'ner']
# TODO have an elegant way of handling spacy tokens vs str tokens
class GlossaryExtractor(Extractor):
    """
    **Description**
        This class takes a list of glossary as reference, extract the matched ngrams string
        from the tokenized input string.

    Examples:
        ::

            glossary = ['Beijing', 'Los Angeles', 'New York', 'Shanghai']
            glossary_extractor = GlossaryExtractor(glossary=glossary,
                                                  ngrams=3,
                                                  case_sensitive=True)
            glossary_extractor.extract(tokens=Tokenizer(input_text))

    """

    def __init__(self,
                 glossary: List[str],
                 extractor_name: str,
                 tokenizer: None,
                 ngrams: int = 2,
                 case_sensitive=False) -> None:
        # if we set tokenizer as None, extractor will use regex to extract tokens to expedite the extraction
        Extractor.__init__(self,
                           input_type=InputType.TOKENS,
                           category="glossary",
                           name=extractor_name)
        self._case_sensitive = case_sensitive
        self._default_tokenizer = tokenizer
        if not ngrams:
            ngrams = 0
            for word in glossary:
                ngrams = max(ngrams, len(self._default_tokenizer.tokenize(word)))
        self._ngrams = min(ngrams, 5)
        self._joiner = " "
        self._glossary = self._populate_trie(glossary)

    def extract(self, tokens: List[Token]) -> List[Extraction]:
        """
        Extracts information from a string(TEXT) with the GlossaryExtractor instance

        Args:
            token (List[Token]): list of spaCy token to be processed.

        Returns:
            List[Extraction]: the list of extraction or the empty list if there are no matches.

        """
        results = list()

        if len(tokens) > 0:
            if self._case_sensitive:
                new_tokens = [x.orth_ if isinstance(x, Token) else x for x in tokens]
            else:
                new_tokens = [x.lower_ if isinstance(x, Token) else x.lower() for x in tokens]
        else:
            return results

        try:
            ngrams_iter = self._generate_ngrams_with_context(new_tokens)
            results.extend(map(lambda term: self._wrap_value_with_context(tokens, term[1], term[2]),
                               filter(lambda term: isinstance(term[0], str),
                                      map(lambda term: (self._glossary.get(term[0]), term[1], term[2]),
                                          map(lambda term: (
                                              self._combine_ngrams(term[0], self._joiner), term[1], term[2]),
                                              ngrams_iter)))))
        except Exception as e:
            raise ExtractorError('GlossaryExtractor: Failed to extract with ' + self.name + '. Catch ' + str(e) + '. ')
        return results

    def _generate_ngrams_with_context(self, tokens: List[Token]) -> chain:
        """Generates the 1-gram to n-grams tuples of the list of tokens"""
        chained_ngrams_iter = self._generate_ngrams_with_context_helper(iter(tokens), 1)
        for n in range(2, self._ngrams + 1):
            ngrams_iter = tee(tokens, n)
            for j in range(1, n):
                for k in range(j):
                    next(ngrams_iter[j], None)
            ngrams_iter_with_context = self._generate_ngrams_with_context_helper(zip(*ngrams_iter), n)
            chained_ngrams_iter = chain(chained_ngrams_iter, ngrams_iter_with_context)
        return chained_ngrams_iter

    def _populate_trie(self, values: List[str]) -> CharTrie:
        """Takes a list and inserts its elements into a new trie and returns it"""
        if self._default_tokenizer:
            return reduce(self._populate_trie_reducer, iter(values), CharTrie())
        return reduce(self._populate_trie_reducer_regex, iter(values), CharTrie())

    def _populate_trie_reducer(self, trie_accumulator=CharTrie(), value="") -> CharTrie:
        """Adds value to trie accumulator"""
        if self._case_sensitive:
            key = self._joiner.join([x.orth_ if isinstance(x, Token) else x for x in
                                     self._default_tokenizer.tokenize(value, disable=disable_spacy)])
        else:
            key = self._joiner.join([x.lower_ if isinstance(x, Token) else x.lower() for x in
                                     self._default_tokenizer.tokenize(value, disable=disable_spacy)])
        trie_accumulator[key] = value
        return trie_accumulator

    def _populate_trie_reducer_regex(self, trie_accumulator=CharTrie(), value="") -> CharTrie:
        """Adds value to trie accumulator"""
        regex = re.compile(r"[A-Za-z0-9]+|[^\w\s]|_")
        if self._case_sensitive:
            key = self._joiner.join([x for x in re.findall(regex, value)])
        else:
            key = self._joiner.join([x.lower() for x in re.findall(regex, value)])
        trie_accumulator[key] = value
        return trie_accumulator

    def _wrap_value_with_context(self, tokens: List[Token], start: int, end: int) -> Extraction:
        """Wraps the final result"""
        return Extraction(' '.join([x.orth_ if isinstance(x, Token) else x for x in tokens[start:end]]),
                          self.name,
                          start_token=start,
                          end_token=end,
                          start_char=tokens[start].idx if isinstance(tokens[start], Token) else -1,
                          end_char=tokens[end - 1].idx + len(tokens[end - 1].orth_) if isinstance(tokens[end - 1],
                                                                                                  Token) else -1
                          )

    @staticmethod
    def _generate_ngrams_with_context_helper(ngrams_iter: iter, ngrams_len: int) -> map:
        """Updates the end index"""
        return map(lambda term: (term[1], term[0], term[0] + ngrams_len), enumerate(ngrams_iter))

    @staticmethod
    def _combine_ngrams(ngrams, joiner) -> str:
        """Construct keys for checking in trie"""
        if isinstance(ngrams, str):
            return ngrams
        else:
            combined = joiner.join(ngrams)
            return combined
