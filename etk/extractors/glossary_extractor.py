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


class GlossaryExtractor(Extractor):
    def __init__(self,
                 glossary: List[str],
                 extractor_name: str,
                 tokenizer: Tokenizer,
                 ngrams: int=2,
                 case_sensitive=False) -> None:
        Extractor.__init__(self,
                           input_type=InputType.TOKENS,
                           category="glossary",
                           name=extractor_name)


        self.case_sensitive = case_sensitive
        self.default_tokenizer = tokenizer
        if not ngrams:
            ngrams = 0
            for word in glossary:
                ngrams = max(ngrams, len(self.default_tokenizer.tokenize(word)))
        self.ngrams = min(ngrams, 5)
        self.joiner = " "
        self.glossary = self.populate_trie(glossary)

    def extract(self, tokens: List[Token]) -> List[Extraction]:
        """Extracts information from a string(TEXT) with the GlossaryExtractor instance"""
        results = list()

        if len(tokens) > 0:
            if self.case_sensitive:
                new_tokens = [x.orth_ for x in tokens]
            else:
                new_tokens = [x.lower_ for x in tokens]
        else:
            return results

        try:
            ngrams_iter = self.generate_ngrams_with_context(new_tokens)
            results.extend(map(lambda term: self.wrap_value_with_context(tokens, term[1], term[2]),
                               filter(lambda term: isinstance(term[0], str),
                                      map(lambda term: (self.glossary.get(term[0]), term[1], term[2]),
                                          map(lambda term: (self.combine_ngrams(term[0], self.joiner), term[1],term[2]), ngrams_iter)))))
        except Exception as e:
            raise ExtractorError('GlossaryExtractor: Failed to extract with ' + self.name + '. Catch ' + str(e) + '. ')
        return results

    def generate_ngrams_with_context(self, tokens: List[Token]) -> chain:
        """Generates the 1-gram to n-grams tuples of the list of tokens"""
        chained_ngrams_iter = self.generate_ngrams_with_context_helper(iter(tokens), 1)
        for n in range(2, self.ngrams + 1):
            ngrams_iter = tee(tokens, n)
            for j in range(1, n):
                for k in range(j):
                    next(ngrams_iter[j], None)
            ngrams_iter_with_context = self.generate_ngrams_with_context_helper(zip(*ngrams_iter), n)
            chained_ngrams_iter = chain(chained_ngrams_iter, ngrams_iter_with_context)
        return chained_ngrams_iter

    def populate_trie(self, values: List[str]) -> CharTrie:
        """Takes a list and inserts its elements into a new trie and returns it"""
        return reduce(self.__populate_trie_reducer, iter(values), CharTrie())

    def __populate_trie_reducer(self, trie_accumulator=CharTrie(), value="") -> CharTrie:
        """Adds value to trie accumulator"""
        if self.case_sensitive:
            key = self.joiner.join([x.orth_ for x in self.default_tokenizer.tokenize(value)])
        else:
            key = self.joiner.join([x.lower_ for x in self.default_tokenizer.tokenize(value)])
        trie_accumulator[key] = value
        return trie_accumulator

    def wrap_value_with_context(self, tokens: List[Token], start: int, end: int) -> Extraction:
        """Wraps the final result"""
        return Extraction(' '.join([x.orth_ for x in tokens[start:end]]),
                          self.name,
                          start_token=start,
                          end_token=end,
                          start_char=tokens[start].idx,
                          end_char=tokens[end-1].idx+len(tokens[end-1].orth_)
                          )

    @staticmethod
    def generate_ngrams_with_context_helper(ngrams_iter: iter, ngrams_len: int) -> map:
        """Updates the end index"""
        return map(lambda term: (term[1], term[0], term[0] + ngrams_len), enumerate(ngrams_iter))

    @staticmethod
    def combine_ngrams(ngrams, joiner) -> str:
        """Construct keys for checking in trie"""
        if isinstance(ngrams, str):
            return ngrams
        else:
            combined = joiner.join(ngrams)
            return combined