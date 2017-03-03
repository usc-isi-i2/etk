# -*- coding: utf-8 -*-
"""Functional way to populate trie"""

"""Module for defining an extractor that accepts a list of tokens
and outputs tokens that exist in a user provided trie"""
import copy
import pygtrie as trie
from itertools import ifilter
from itertools import tee
from itertools import chain
from itertools import izip
from itertools import imap
from itertools import repeat
from pygtrie import CharTrie


def __populate_trie_reducer(trie_accumulator=trie.CharTrie(), value=""):
    """Adds value to trie accumulator"""
    trie_accumulator[value] = value
    return trie_accumulator


def populate_trie(values):
    """Takes a list and inserts its elements into a new trie and returns it"""
    return reduce(__populate_trie_reducer, iter(values), trie.CharTrie())


# class DictionaryExtractor():

#     def __init__(self, pre_process = lambda x: x, 
#     				pre_filter = lambda x: x, 
#     				post_filter = lambda x: isinstance(x, basestring),
#     				trie = None,
#     				ngrams = 1,
#     				joiner = ' '):
#         self.pre_process = pre_process
#         self.pre_filter = pre_filter
#         self.post_filter = post_filter
#         self.trie = trie
#         self.ngrams = ngrams
#         self.joiner = joiner 

#     def wrap_value_with_context(self, value, field, start, end):
#         return {'value': value,
#                 'context': {'field': field,
#                             'start': start,
#                             'end': end
#                             }
#                 }

#     def generate_ngrams_with_context_helper(self,
#                                             ngrams_iterable,
#                                             ngrams_length):
#         return imap(lambda ngrams_with_index: (ngrams_with_index[1],
#                                                ngrams_with_index[0],
#                                                ngrams_with_index[0] +
#                                                ngrams_length),
#                     enumerate(ngrams_iterable))

#     def generate_ngrams_with_context(self, tokens):
#         chained_ngrams_iterable = self.generate_ngrams_with_context_helper(iter(tokens), 1)
#         for n in range(2, self.ngrams + 1):
#             ngrams_iterable = tee(tokens, n)
#             for j in range(1, n):
#                 for k in range(j):
#                     next(ngrams_iterable[j], None)
#             ngrams_iterable_with_context = self.generate_ngrams_with_context_helper(izip(*ngrams_iterable), n)
#             chained_ngrams_iterable = chain(chained_ngrams_iterable,
#                                             ngrams_iterable_with_context)

#         return chained_ngrams_iterable

#     def combine_ngrams(self, ngrams):
#         if isinstance(ngrams, basestring):
#             return ngrams
#         else:
#             combined = self.joiner.join(ngrams)
#             return combined

#     def extract(self, tokens):
#         try:
#             extracts = list()

#             ngrams_iterable = self.generate_ngrams_with_context(tokens)
#             extracts.extend(
#                 map(lambda ngrams_context: self.wrap_value_with_context(ngrams_context[0], 'tokens', ngrams_context[1], ngrams_context[2]),
#                     ifilter(lambda ngrams_context: self.post_filter(ngrams_context[0]),
#                             map(lambda ngrams_context: (self.trie.get(ngrams_context[0]), ngrams_context[1], ngrams_context[2]),
#                                         ifilter(lambda ngrams_context: self.pre_filter(ngrams_context[0]),
#                                                 map(lambda ngrams_context: (self.pre_process(ngrams_context[0]), ngrams_context[1], ngrams_context[2]),
#                                                     map(lambda ngrams_context: (self.combine_ngrams(ngrams_context[0]), ngrams_context[1], ngrams_context[2]), ngrams_iterable)))))))

#             return list(extracts)

#         except Exception as inst:
#             print "error operator"
#             print type(inst)
#             print inst
#             return list()



def wrap_value_with_context(value, field, start, end):
    return {'value': value,
            'context': {'field': field,
                        'start': start,
                        'end': end
                        }
            }

def generate_ngrams_with_context_helper(ngrams_iterable,
                                        ngrams_length):
    return imap(lambda ngrams_with_index: (ngrams_with_index[1],
                                           ngrams_with_index[0],
                                           ngrams_with_index[0] +
                                           ngrams_length),
                enumerate(ngrams_iterable))

def generate_ngrams_with_context(tokens, ngrams):
    chained_ngrams_iterable = generate_ngrams_with_context_helper(iter(tokens), 1)
    for n in range(2, ngrams + 1):
        ngrams_iterable = tee(tokens, n)
        for j in range(1, n):
            for k in range(j):
                next(ngrams_iterable[j], None)
        ngrams_iterable_with_context = generate_ngrams_with_context_helper(izip(*ngrams_iterable), n)
        chained_ngrams_iterable = chain(chained_ngrams_iterable,
                                        ngrams_iterable_with_context)

    return chained_ngrams_iterable

def combine_ngrams(ngrams, joiner):
    if isinstance(ngrams, basestring):
        return ngrams
    else:
        combined = joiner.join(ngrams)
        return combined

def extract_using_dictionary(tokens, pre_process = lambda x: x, 
				pre_filter = lambda x: x, 
				post_filter = lambda x: isinstance(x, basestring),
				trie = None,
				ngrams = 1,
				joiner = ' '):

    field = 'tokens'

    if isinstance(tokens[0], dict):
        tokens = [x["value"] for x in tokens]
        field = 'structured_tokens'

    try:
        extracts = list()

        ngrams_iterable = generate_ngrams_with_context(tokens, ngrams)
        extracts.extend(
            map(lambda ngrams_context: wrap_value_with_context(ngrams_context[0], field, ngrams_context[1], ngrams_context[2]),
                ifilter(lambda ngrams_context: post_filter(ngrams_context[0]),
                        map(lambda ngrams_context: (trie.get(ngrams_context[0]), ngrams_context[1], ngrams_context[2]),
                                    ifilter(lambda ngrams_context: pre_filter(ngrams_context[0]),
                                            map(lambda ngrams_context: (pre_process(ngrams_context[0]), ngrams_context[1], ngrams_context[2]),
                                                map(lambda ngrams_context: (combine_ngrams(ngrams_context[0], joiner), ngrams_context[1], ngrams_context[2]), ngrams_iterable)))))))

        return list(extracts)

    except Exception as inst:
        print "error operator"
        print type(inst)
        print inst
        return list()
