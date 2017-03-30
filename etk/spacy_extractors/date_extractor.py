from __future__ import unicode_literals, print_function
import json, re
from pathlib import Path

import spacy
from spacy.matcher import Matcher
from spacy.attrs import *

ordinals = ['rd', 'st', 'th', 'nd']
months_dict = {
        "01": 1,
        "1": 1,
        "02": 2,
        "2": 2,
        "03": 3,
        "3": 3,
        "04": 4,
        "4": 4,
        "05": 5,
        "5": 5,
        "06": 6,
        "6": 6,
        "07": 7,
        "7": 7,
        "08": 8,
        "8": 8,
        "09": 9,
        "9": 9,
        "10": 10,
        "11": 11,
        "12": 12,
        "january": 1,
        "february": 2,
        "march": 3,
        "april": 4,
        "may": 5,
        "june": 6,
        "july": 7,
        "august": 8,
        "september": 9,
        "october": 10,
        "november": 11,
        "december": 12,
        "jan": 1,
        "feb": 2,
        "mar": 3,
        "apr": 4,
        "jun": 6,
        "jul": 7,
        "aug": 8,
        "sep": 9,
        "oct": 10,
        "nov": 11,
        "de": 12,
        "enero": 1,
        "febrero": 2,
        "marzo": 3,
        "abril": 4,
        "mayo": 5,
        "junio": 6,
        "julio": 7,
        "agosto": 8,
        "septiembre": 9,
        "octubre": 10,
        "noviembre": 11,
        "diciembre": 12,
        "janvier": 1,
        "fevrier": 2,
        "fvrier": 2,
        "mars": 3,
        "avril": 4,
        "mai": 5,
        "juin": 6,
        "juillet": 7,
        "aout": 8,
        "aot": 8,
        "septembre": 9,
        "octobre": 10,
        "novembre": 11,
        "decempre": 12,
        "janeiro": 1,
        "fevereiro": 2,
        "marco": 3,
        # "abril": 4,
        "maio": 5,
        "junho": 6,
        "julho": 7,
        # "agosto": 8,
        "setembro": 9,
        "setiembre": 9,
        "outubro": 10,
        "novembro": 11,
        "dezembro": 12,
        "gennaio": 1,
        "febbraio": 2,
        # "marzo": 3,
        "aprile": 4,
        "maggio": 5,
        "giugno": 6,
        "luglio": 7,
        # "agosto": 8,
        "settembre": 9,
        "ottobre": 10,
        # "novembre": 11,
        "dicembre": 12,
        "januar": 1,
        # "februar": 2,
        "marz": 3,
        # "april": 4,
        # "mai": 5,
        "juni": 6,
        "juli": 7,
        # "august": 8,
        # "september": 9,
        "oktober": 10,
        # "november": 11,
        "dezember": 12,
        # "januar": 1,
        # "februar": 2,
        "marts": 3,
        # "april": 4,
        "maj": 5,
        # "juni": 6,
        # "juli": 7,
        # "august": 8,
        # "september": 9,
        # "oktober": 10,
        # "november": 11,
        # "december": 12
    }

def load_date_matcher(nlp):

	# Create matcher object with list of rules and return
	matcher = Matcher(nlp.vocab)

	#Add rules
	matcher.add_pattern('DATE', 
	                    [{POS : 'NUM', LENGTH : 2}, {ORTH: "/", 'OP' : '+'}, {POS : 'NUM'}, {ORTH: "/", 'OP' : '+'}, {POS : 'NUM'}])
	matcher.add_pattern('DATE', 
	                    [{POS : 'NUM'}, {ORTH: "-", 'OP' : '+'}, {POS : 'NUM'}, {ORTH: "-", 'OP' : '+'}, {POS : 'NUM'}])

	# March 25, 2017
	# March 25th, 2017
	# March 25th 2017
	# March 25 2017
	# 25 March, 2017
	# 25th March, 2017
	# 25th March 2017
	# 25 March 2017
	for month in months_dict.keys():		
		for ordinal in ordinals:
			matcher.add_pattern('DATE',
				[
					{LOWER : month}, 
					{POS : 'NUM', LENGTH : 1}, 
					{LOWER : ordinal, 'OP' : '?'},
					{ORTH : ',', 'OP' : '?'},
					{POS : 'NUM', LENGTH : 4}
				])
			matcher.add_pattern('DATE',
				[
					{LOWER : month}, 
					{POS : 'NUM', LENGTH : 2}, 
					{LOWER : ordinal, 'OP' : '?'},
					{ORTH : ',', 'OP' : '?'},
					{POS : 'NUM', LENGTH : 4}
				])

			matcher.add_pattern('DATE',
				[
					{POS : 'NUM', LENGTH : 1}, 
					{LOWER : month}, 
					{LOWER : ordinal, 'OP' : '?'},
					{ORTH : ',', 'OP' : '?'},
					{POS : 'NUM', LENGTH : 4}
				])
			matcher.add_pattern('DATE',
				[ 
					{POS : 'NUM', LENGTH : 2},
					{LOWER : month}, 
					{LOWER : ordinal, 'OP' : '?'},
					{ORTH : ',', 'OP' : '?'},
					{POS : 'NUM', LENGTH : 4}
				])

	return matcher

def remove_ordinals(tokens):
    for i in range(len(tokens)):
        tokens[i] = re.sub(r'(\d)(st|nd|rd|th)', r'\1', tokens[i])
    return tokens
        
# def replace_tokenizer(nlp, custom_split_function):
#     spaCy_tokenizer = nlp.tokenizer 
#     nlp.tokenizer = lambda string: spaCy_tokenizer.tokens_from_list(remove_ordinals(custom_split_function(string)))

def replace_tokenizer(nlp, tk):
    spacy_tokenizer = nlp.tokenizer 
    nlp.tokenizer = lambda string: spacy_tokenizer.tokens_from_list(remove_ordinals(tk.extract_tokens_from_crf(tk.extract_crftokens(string))))

def extract_date_spacy(nlp, matcher, tk, string):

	# Override tokenizer
	replace_tokenizer(nlp, tk)
	doc = nlp(string)

	# Run matcher and return results
	extracted_dates = []
	extractions = set()
	matches = matcher(doc)
	for ent_id, label, start, end in matches:
		extractions.add((start, end))
	for extraction in extractions:
		extracted_dates.append({'value' : (doc[extraction[0] : extraction[1]].text)})

	# Return the results
	return extracted_dates