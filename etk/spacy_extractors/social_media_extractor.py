# coding: utf-8

from spacy.matcher import Matcher
from spacy.attrs import IS_DIGIT, FLAG55, FLAG54, LOWER, IS_PUNCT, LENGTH, SUFFIX, IS_ASCII, TAG
import re


def add_to_vocab(nlp, lst):
    for lexeme in lst:
        nlp.vocab[lexeme.lower().decode('utf8')]


def load_social_media_matcher(nlp):

    social_media = ['twitter', 'facebook', 'instagram', 'wechat', 'viber', 'line', 'whatsapp', 'snapchat']
    separators = [':', '-', '@']
    add_to_vocab(nlp, social_media)
    add_to_vocab(nlp, separators)

    is_separator = FLAG55
    is_social_media = FLAG54
    social_media_ids = {nlp.vocab.strings[s.lower()] for s in social_media}
    separators_ids = {nlp.vocab.strings[s.lower()] for s in separators}

    for lexeme in nlp.vocab:
        if lexeme.lower in social_media_ids:
            lexeme.set_flag(is_social_media, True)
        if lexeme.lower in separators_ids:
            lexeme.set_flag(is_separator, True)

    matcher = Matcher(nlp.vocab)
    matcher.add_entity("social_media")

    matcher.add_pattern("social_media",
                        [
                            {is_social_media: True},
                            {is_separator: True},
                            {is_separator: True, 'OP': '?'},
                            {
                                IS_ASCII: True
                            }
                        ],
                        label=1
                        )

    matcher.add_pattern("social_media",
                        [
                            {is_social_media: True},
                            {
                                LOWER: "me",
                                TAG: "PRP"
                            },
                            {is_separator: True, "OP": '?'},
                            {
                                IS_ASCII: True,
                                TAG: 'NN'
                            }
                        ],
                        label=2
                        )
    '''matcher.add_pattern("social_media",
                                        [
                                         {is_social_media: True},
                                         {
                                             LOWER: "me",
                                             TAG: "PRP"
                                         },
                                         {is_separator: True, "OP": '?'},
                                         {IS_ASCII: False, "OP":"?"},
                                        ],
                                         label = 3
                                        )
    '''
    matcher.add_pattern("social_media",
                        [
                            {is_social_media: True},
                            {LOWER: 'id'},
                            {LOWER: 'is', 'OP': '?'},
                            {is_separator: True, 'OP': '?'},
                            {IS_ASCII: True}
                        ],
                        label=4
                        )

    matcher.add_pattern("social_media",
                        [
                            {is_social_media: True},
                            {TAG: 'NN'},
                            {LOWER: 'is', 'OP': '?'},
                            {LOWER: 'to'},
                            {TAG: 'VB'},
                            {LOWER: 'me'}
                        ],
                        label=5
                        )

    matcher.add_pattern("social_media",
                        [
                            {LOWER: 'add'},
                            {TAG: 'PRP'},
                            {LOWER: 'on'},
                            {is_social_media: True},
                            {TAG: 'NN'}
                        ],
                        label=6
                        )

    return matcher


# Postprocessing the matches - extracting the ages from the matches
def post_process(matches, nlp_doc):
    extractions = dict()

    for ent_id, label, start, end in matches:
        if label in [1, 2, 3, 4]:
            social_media, handle = str(nlp_doc[start]).strip(), str(nlp_doc[end - 1]).strip()
        if label in [5]:
            social_media, handle = str(nlp_doc[start]).strip(), str(nlp_doc[start + 1]).strip()
        if label in [6]:
            social_media, handle = str(nlp_doc[end - 2]).strip(), str(nlp_doc[end - 1]).strip()

        if social_media not in extractions:
            extractions[social_media] = {handle: (start, end)}
        elif handle not in extractions[social_media]:
            extractions[social_media][handle] = (start, end)
    return extractions


def wrap_value_with_context(social_media, handle, start, end):
    return {
        'value': str(handle),
        'context':
            {
                "start": start,
                "end": end
        },
        'metadata':
            {
                "social_network": str(social_media),
            }
    }


def extract(nlp_doc, matcher):

    social_media_matches = matcher(nlp_doc)

    extractions = post_process(social_media_matches, nlp_doc)

    social_media_extracts = list()

    for social_media in extractions:
        for e in extractions[social_media]:
            handle = e
            start, end = extractions[social_media][e]
            social_media_extracts.append(wrap_value_with_context(social_media, handle, start, end))

    return social_media_extracts
