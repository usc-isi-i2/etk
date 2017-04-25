# coding: utf-8

from spacy.matcher import Matcher
from spacy.attrs import IS_DIGIT, FLAG55, FLAG54, LOWER, IS_PUNCT, LENGTH, SUFFIX, IS_ASCII
import re


def add_to_vocab(nlp, lst):
    for lexeme in lst:
        nlp.vocab[lexeme.lower().decode('utf8')]


def load_social_media_matcher(nlp):

    matcher = Matcher(nlp.vocab)

    social_media = ['twitter', 'facebook', 'instagram', 'wechat', 'viber', 'line', 'whatsapp', 'snapchat']
    separators = [':', '-', '@']
    is_separator = FLAG54
    is_social_media = FLAG55
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
                                         {IS_ASCII: True}
                                        ],
                                         label = 1
                                        )

    matcher.add_pattern("social_media",
                                        [
                                         {is_social_media: True},
                                         {LOWER: "me"},
                                         {IS_PUNCT: True, "OP": '?'},
                                         {IS_ASCII: True}
                                        ],
                                         label = 2
                                        )
    matcher.add_pattern("social_media",
                                        [
                                         {is_social_media: True},
                                         {LOWER: "me"}, 
                                         {IS_PUNCT: True, "OP": '?'}, 
                                         {IS_ASCII: False}
                                        ],
                                         label = 3
                                        )
    matcher.add_pattern("social_media",
                                        [
                                         {is_social_media: True}, 
                                         {LOWER: 'id'}, 
                                         {LOWER: 'is', 'OP': '?'}, 
                                         {is_separator: True, 'OP': '?'},
                                         {IS_ASCII: True}
                                        ],
                                         label = 4
                                        )

    return matcher


# Postprocessing the matches - extracting the ages from the matches
def post_process(matches, nlp_doc):
    handles = dict()
    for ent_id, label, start, end in matches:
        if(label in [1,2,3,4]):
            handle = nlp_doc[end-1]
            social_media_network = nlp_doc[start]
            handles[handle] = {"start":start,"end":end,"social_network":social_media_network}

    return handles


def wrap_value_with_context(handle):
    return {
        'value': handle[0],
        'context':
            {
                "social_network": handle[1]['social_network'],
                "start": handle[1]['start'],
                "end": handle[1]['end']
        }
    }


def extract(nlp_doc, matcher):

    social_media_matches = matcher(nlp_doc)

    handles = post_process(social_media_matches, nlp_doc)

    social_media_extracts = [wrap_value_with_context(handle) for handle in handles.items()]

    return social_media_extracts
