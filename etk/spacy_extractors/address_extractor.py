# coding: utf-8

import re
import spacy
from spacy.matcher import Matcher
from spacy.attrs import FLAG58, FLAG57, FLAG56, POS, ORTH, LENGTH, LOWER, IS_DIGIT, IS_ASCII, LIKE_NUM, IS_ALPHA

street = ["avenue", "blvd", "boulevard", "pkwy", "parkway", "way",
          "st", "street", "rd", "road", "drive", "lane", "alley", "ave"]

separator = ["and", "/", "\\", "-", "&"]

street_name = list()
for i in range(100):
    if i in range(11, 20):
        street_name.append(str(i) + 'th')
        street_name.append('0' + str(i) + 'th')
    if i % 10 == 1:
        street_name.append(str(i) + 'st')
        street_name.append('0' + str(i) + 'st')
    elif i % 10 == 2:
        street_name.append(str(i) + 'nd')
        street_name.append('0' + str(i) + 'nd')
    elif i % 10 == 1:
        street_name.append(str(i) + 'rd')
        street_name.append('0' + str(i) + 'rd')
    else:
        street_name.append(str(i) + 'th')
        street_name.append('0' + str(i) + 'th')


def add_to_vocab(nlp, lst):
    for lexeme in lst:
        nlp.vocab[lexeme.lower().decode('utf8')]


def load_address_matcher(nlp):

    # Create matcher object with list of rules and return
    matcher = Matcher(nlp.vocab)

    # Add to vocab
    add_to_vocab(nlp, street)
    add_to_vocab(nlp, street_name)

    # Create flag for MONTH
    is_street = FLAG58
    street_ids = {nlp.vocab.strings[
        s.lower()] for s in street}

    is_separator = FLAG57
    separator_ids = {nlp.vocab.strings[
        s.lower()] for s in separator}

    is_street_name = FLAG56
    street_name_ids = {nlp.vocab.strings[
        s.lower()] for s in street_name}

    # Add the flags
    for lexeme in nlp.vocab:
        if lexeme.lower in street_ids:
            lexeme.set_flag(is_street, True)
        if lexeme.lower in separator_ids:
            lexeme.set_flag(is_separator, True)
        if lexeme.is_alpha:
            lexeme.set_flag(is_street_name, True)
        if lexeme.like_num:
            lexeme.set_flag(is_street_name, True)
        if lexeme.lower in street_name_ids:
            lexeme.set_flag(is_street_name, True)

    # Add rules
    street_name_rules = [
        {is_street_name: True},
        {IS_ALPHA: True}
    ]
    for street_name_rule in street_name_rules:
        for length in range(1, 6):
            # direct address
            matcher.add_pattern('ADDRESS',
                                [
                                    {LIKE_NUM: True, LENGTH: length},
                                    street_name_rule,
                                    {is_street: True}
                                ])
            matcher.add_pattern('ADDRESS',
                                [
                                    {LIKE_NUM: True, LENGTH: length},
                                    {IS_ALPHA: True},
                                    street_name_rule,
                                    {is_street: True}
                                ])
            matcher.add_pattern('ADDRESS',
                                [
                                    {LIKE_NUM: True, LENGTH: length},
                                    {IS_ALPHA: True},
                                    {IS_ALPHA: True},
                                    street_name_rule,
                                    {is_street: True}
                                ])

        # Add and filter out matches to return longest match
        matcher.add_pattern('ADDRESS',
                            [
                                street_name_rule,
                                {is_street: True}
                            ])

    # two street rules
    for street_name_rule1 in street_name_rules:
        for street_name_rule2 in street_name_rules:
            matcher.add_pattern('ADDRESS',
                                [
                                    {LIKE_NUM: True},
                                    street_name_rule1,
                                    {is_street: True},
                                    {is_separator: True},
                                    {LIKE_NUM: True},
                                    street_name_rule2,
                                    {is_street: True}
                                ])
            matcher.add_pattern('ADDRESS',
                                [
                                    street_name_rule1,
                                    {is_street: True},
                                    {is_separator: True},
                                    street_name_rule2,
                                    {is_street: True}
                                ])

    return matcher


def extract(doc, matcher):

    # print [(word.text, word.pos_) for word in doc]

    # Run matcher and return results
    extracted_addresses = list()
    extractions = list()
    count = 0

    address_matches = matcher(doc)

    for ent_id, label, start, end in address_matches:
        if count != 0:
            prev_start, prev_end = extractions[count - 1]
            if (start == prev_start) and (end > prev_end):
                extractions[count - 1][1] = end
            elif (start > prev_start) and (end > prev_end):
                extractions.append([start, end])
                count += 1
        else:
            extractions.append([start, end])
            count += 1

    for extraction in extractions:
        start, end = extraction
        extracted_address = {'context': {}}
        extracted_address['value'] = doc[start:end].text
        extracted_address['context'] = {'start': start, 'end': end}
        extracted_addresses.append(extracted_address)

    # Return the results
    return extracted_addresses
