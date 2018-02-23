from etk2.extractor import Extractor
from etk2.etk_extraction import ExtractionCollection, Extraction
from etk2.tokenizer import Tokenizer
from etk2.segment import Segment, SegmentCollection
from etk2.etk import ETK

import pygtrie as trie
from itertools import *
from functools import reduce

class GlossaryExtractor(Extractor):
    def __init__(self, glossary, ngrams=2, case_sensitive=False):
        self.glossary = glossary
        self.ngrams = ngrams
        self.case_sensitive = case_sensitive

    @property
    def input_type(self):
        """
        The type of input that an extractor wants
        Returns: InputType
        """
        return self.InputType.TEXT

    # @property
    def name(self):
        if self.glossary.name is None:
            return "unknown glossary extractor"
        return self.glossary.name + " extractor"

    # @property
    def category(self):
        return "glossary"

    def extract(self, extractables):
        preferred_tokenizer = self.preferred_tokenizer()
        pre_process = lambda x: x
        pre_filter = lambda x: x
        post_filter = lambda x: isinstance(x, str)
        trie = self.glossary
        ngrams = self.ngrams
        joiner = ' '

        print(self.glossary)

        for item in extractables.items():
            tokens = item.get_tokens(tokenizer=preferred_tokenizer)
            results = ExtractionCollection()

            if len(tokens) > 0:
                tokens = [x.orth_ for x in tokens]
            try:
                ngrams_iterable = self.generate_ngrams_with_context(tokens, ngrams)
                temp_res = map(lambda ngrams_context: self.wrap_value_with_context(ngrams_context[0], ngrams_context[1],
                                                                       ngrams_context[2]),
                        filter(lambda ngrams_context: post_filter(ngrams_context[0]),
                               map(lambda ngrams_context: (
                                   trie.get(ngrams_context[0]), ngrams_context[1], ngrams_context[2]),
                                   filter(lambda ngrams_context: pre_filter(ngrams_context[0]),
                                          map(lambda ngrams_context: (
                                              pre_process(ngrams_context[0]), ngrams_context[1], ngrams_context[2]),
                                              map(lambda ngrams_context: (
                                                  self.combine_ngrams(ngrams_context[0], joiner), ngrams_context[1],
                                                  ngrams_context[2]), ngrams_iterable))))))
                for x in temp_res:
                    results.add(x)
            except Exception as inst:
                print("error operator")
                continue
        return results

    def generate_ngrams_with_context(self, tokens, ngrams):
        chained_ngrams_iterable = self.generate_ngrams_with_context_helper(iter(tokens), 1)
        for n in range(2, ngrams + 1):
            ngrams_iterable = tee(tokens, n)
            for j in range(1, n):
                for k in range(j):
                    next(ngrams_iterable[j], None)
            ngrams_iterable_with_context = self.generate_ngrams_with_context_helper(zip(*ngrams_iterable), n)
            chained_ngrams_iterable = chain(chained_ngrams_iterable,
                                            ngrams_iterable_with_context)
        return chained_ngrams_iterable

    def preferred_tokenizer(self):
        """
        Returns: a tokenizer object to use for tokenizing the input segment used for extraction.
        The same tokenizer should be used during initialization to tokenize the glossary entries
        so that there is no mismatch on how text is tokenized when fed to the glossary extractor.

        """
        crf_tokenizer = Tokenizer()
        return crf_tokenizer

    @staticmethod
    def populate_trie(values):
        """Takes a list and inserts its elements into a new trie and returns it"""
        def __populate_trie_reducer(trie_accumulator=trie.CharTrie(), value=""):
            """Adds value to trie accumulator"""
            trie_accumulator[value] = value
            return trie_accumulator
        return reduce(__populate_trie_reducer, iter(values), trie.CharTrie())

    @staticmethod
    def wrap_value_with_context(value, start, end):
        return Extraction({'value': value,
                'context': {'start': start,
                            'end': end
                            }
                })

    @staticmethod
    def generate_ngrams_with_context_helper(ngrams_iterable,
                                            ngrams_length):
        return map(lambda ngrams_with_index: (ngrams_with_index[1],
                                              ngrams_with_index[0],
                                              ngrams_with_index[0] +
                                              ngrams_length),
                   enumerate(ngrams_iterable))

    @staticmethod
    def combine_ngrams(ngrams, joiner):
        if isinstance(ngrams, str):
            return ngrams
        else:
            combined = joiner.join(ngrams)
            return combined


# ==== for test only ====
doc = SegmentCollection()
doc.add(Segment('*', 'i live in shanghai'))

etk = ETK()

aaa = GlossaryExtractor(glossary=etk.get_glossary('cities.txt'), ngrams=2)
bbb = aaa.extract(doc)
print(bbb.all_values())