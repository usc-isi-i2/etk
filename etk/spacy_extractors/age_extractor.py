# coding: utf-8

from spacy.matcher import Matcher
from spacy.attrs import IS_DIGIT, FLAG63, LOWER, IS_PUNCT, LENGTH, SUFFIX, IS_ASCII
import re


def add_to_vocab(nlp, lst):
    for lexeme in lst:
        nlp.vocab[lexeme.lower().decode('utf8')]


def load_age_matcher(nlp):
    """
    Matcher Handles:
    Age : 22 years
    age : 22 yrs
    Age 22-40
    22 yrs
    23yrs
    22-40 years
    About me 22
    """

    matcher = Matcher(nlp.vocab)

    # Added New attribute to check for years
    years = ['years', 'yrs', 'year']
    add_to_vocab(nlp, years)
    is_year = FLAG63
    target_ids = {nlp.vocab.strings[s.lower()] for s in years}
    for lexeme in nlp.vocab:
        if lexeme.lower in target_ids:
            lexeme.set_flag(is_year, True)

    # New Entity Type : Age
    matcher.add_entity("Age")

    # Age Matcher Patterns
    matcher.add_pattern("Age", [{LOWER: "age"}, {IS_PUNCT: True, 'OP': '?'}, {
                        IS_DIGIT: True, LENGTH: 2}])

    matcher.add_pattern("Age", [{LOWER: "age"}, {IS_PUNCT: True}, {IS_DIGIT: True, LENGTH: 2}, {IS_PUNCT: True},
                                {IS_DIGIT: True, LENGTH: 2}])
    matcher.add_pattern("Age", [{LOWER: "age"}, {IS_DIGIT: True, LENGTH: 2}, {
                        IS_PUNCT: True}, {IS_DIGIT: True, LENGTH: 2}])

    matcher.add_pattern("Age", [{IS_DIGIT: True, LENGTH: 2}, {is_year: True}])

    matcher.add_pattern("Age", [{SUFFIX: "yrs", LENGTH: 5}])

    matcher.add_pattern("Age", [{IS_DIGIT: True, LENGTH: 2}, {IS_PUNCT: True, 'OP': '?'}, {IS_DIGIT: True, LENGTH: 2},
                                {is_year: True}])
    matcher.add_pattern("Age", [{IS_DIGIT: True, LENGTH: 2}, {IS_ASCII: True, 'OP': '?'}, {IS_DIGIT: True, LENGTH: 2},
                                {is_year: True}])

    matcher.add_pattern(
        "Age", [{LOWER: 'about'}, {LOWER: 'me', 'OP': '?'}, {IS_DIGIT: True}])

    return matcher


# Postprocessing the matches - extracting the ages from the matches
def post_process(matches, nlp_doc):
    ages = dict()
    for ent_id, label, start, end in matches:
        age = re.findall('\d\d', str(nlp_doc[start:end]))

        for a in age:
            if a not in ages:
                ages[a] = {'start': start, 'end': end}
    return ages


def wrap_value_with_context(age):
    return {
        'value': age[0],
        'context':
            {
                "start": age[1]['start'],
                "end": age[1]['end']
        }
    }


def extract(nlp_doc, matcher):

    age_matches = matcher(nlp_doc)

    processed_matches = post_process(age_matches, nlp_doc)

    extracts = [wrap_value_with_context(age)
                for age in processed_matches.items()]

    return extracts
