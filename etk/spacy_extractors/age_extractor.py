# coding: utf-8

from spacy.matcher import Matcher
from spacy.attrs import IS_DIGIT, FLAG63, LOWER, IS_PUNCT, LENGTH, SUFFIX, IS_ASCII
import re


# Acceptor Function
def get_age(doc, ent_id, label, start, end):
    num_ages = 0
    index = 0
    for i in range(start, end):
        if doc[i].check_flag(IS_DIGIT):
            num_ages += 1
            index = i
    if num_ages == 1:
        return ent_id, label, index, index + 1
    return ent_id, label, start, end


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
    is_year = FLAG63
    target_ids = {nlp.vocab.strings[s.lower()] for s in years}
    for lexeme in nlp.vocab:
        if lexeme.lower in target_ids:
            lexeme.set_flag(is_year, True)

    # New Entity Type : Age
    matcher.add_entity("Age", acceptor=get_age)

    # Age Matcher Patterns
    matcher.add_pattern("Age", [{LOWER: "age"}, {IS_PUNCT: True}, {IS_DIGIT: True, LENGTH: 2}])
    matcher.add_pattern("Age", [{LOWER: "age"}, {IS_DIGIT: True, LENGTH: 2}])

    matcher.add_pattern("Age", [{LOWER: "age"}, {IS_PUNCT: True}, {IS_DIGIT: True, LENGTH: 2}, {IS_PUNCT: True},
                                {IS_DIGIT: True, LENGTH: 2}])
    matcher.add_pattern("Age",
                        [{LOWER: "age"}, {IS_DIGIT: True, LENGTH: 2}, {IS_PUNCT: True}, {IS_DIGIT: True, LENGTH: 2}])

    matcher.add_pattern("Age", [{IS_DIGIT: True, LENGTH: 2}, {is_year: True}])

    matcher.add_pattern("Age", [{SUFFIX: "yrs", LENGTH: 5}])

    matcher.add_pattern("Age", [{IS_DIGIT: True, LENGTH: 2}, {IS_PUNCT: True, 'OP': '?'}, {IS_DIGIT: True, LENGTH: 2},
                                {is_year: True}])
    matcher.add_pattern("Age", [{IS_DIGIT: True, LENGTH: 2}, {IS_ASCII: True, 'OP': '?'}, {IS_DIGIT: True, LENGTH: 2},
                                {is_year: True}])

    matcher.add_pattern("Age", [{LOWER: 'about'}, {LOWER: 'me', 'OP': '?'}, {IS_DIGIT: True}])

    return matcher


# Preprocessing the document - removing extra whitespaces
def preprocess_doc(doc):
    doc = doc.replace('\n', '')
    doc = doc.replace('\r', '')
    doc = re.sub(' +', ' ', doc)
    return doc


# Postprocessing the matches - extracting the ages from the matches
def postprocess(matches, doc):
    ages = set()
    for ent_id, label, start, end in matches:
        ages.update(re.findall('\d\d', str(doc[start:end])))
    return ages


def wrap_value_with_context(age, doc):
    return {
        'value': age,
        'context':
            {
                'start': doc.index(age),
                'end': doc.index(age) + len(age)
            }
    }


def extract(text, nlp, matcher):
    text = preprocess_doc(text)
    nlp_doc = nlp(text)

    age_matches = matcher(nlp_doc)

    processed_matches = postprocess(age_matches, nlp_doc)

    extracts = [wrap_value_with_context(age, text) for age in processed_matches]

    return extracts
