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

    matcher.add_pattern("social_media", [{is_social_media: True}, {is_separator: True}, {is_separator: True, 'OP': '?'},
                                         {IS_ASCII: True}])
    matcher.add_pattern("social_media",
                        [{is_social_media: True}, {LOWER: "me"}, {IS_PUNCT: True, "OP": '?'}, {IS_ASCII: True}])
    matcher.add_pattern("social_media",
                        [{is_social_media: True}, {LOWER: "me"}, {IS_PUNCT: True, "OP": '?'}, {IS_ASCII: False}])
    matcher.add_pattern("social_media",
                        [{is_social_media: True}, {LOWER: 'id'}, {LOWER: 'is', 'OP': '?'}, {is_separator: True, 'OP': '?'},
                         {IS_ASCII: True}])

    return matcher


# Postprocessing the matches - extracting the ages from the matches
def post_process(matches, nlp_doc):
    handles = dict()
    for ent_id, label, start, end in matches:
        handle = re.findall('\d\d', str(nlp_doc[start:end]))

        for a in handle:
            if a not in handles:
                handles[a] = {'start': start, 'end': end}
    return handles


def wrap_value_with_context(handle):
    return {
        'value': handle[0],
        'context':
            {
                "start": handle[1]['start'],
                "end": handle[1]['end']
        }
    }


def extract(nlp_doc, matcher):

    social_media_matches = matcher(nlp_doc)

    #processed_matches = post_process(age_matches, nlp_doc)

    #extracts = [wrap_value_with_context(age) for age in processed_matches.items()]

    return social_media_matches
