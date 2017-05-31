from spacy.matcher import Matcher
from spacy.attrs import LOWER, IS_ASCII, IS_DIGIT, FLAG18, FLAG19, FLAG20, TAG, DEP, ENT_TYPE

def add_to_vocab(nlp, lst):
    for lexeme in lst:
        nlp.vocab[lexeme.lower().decode('utf8')]

def load_movement_matcher(nlp):
	matcher = Matcher(nlp.vocab)

	place = ['area', 'place', 'city', 'town']
	girl = ['gal', 'girl', 'slut', 'cutie', 'hottie', 'lady', 'teen', 'teenager', 'chick', 'staff', 'gf', 'she']

	add_to_vocab(nlp, place)
	add_to_vocab(nlp, girl)
	is_place = FLAG18
	is_girl = FLAG19
	upper_start = FLAG20
	place_ids = {nlp.vocab.strings[s.lower()] for s in place}
	girl_ids = {nlp.vocab.strings[s.lower()] for s in girl}
	for lexeme in nlp.vocab:
	    if lexeme.lower in place_ids:
	        lexeme.set_flag(is_place, True)
	    if lexeme.lower in girl_ids:
	        lexeme.set_flag(is_girl, True)
	    if lexeme.prefix_.isupper():
	    	lexeme.set_flag(upper_start, True)

    # Positive Matcher Patterns
	matcher.add_entity(1)
	matcher.add_pattern(1, [{LOWER: "last"}, {LOWER: "night"}, {LOWER: "in"}, {LOWER: "town"}])
	matcher.add_pattern(1, [{LOWER: "leave"}, {ENT_TYPE: "DATE"}])
	matcher.add_pattern(1, [{LOWER: "leave"}, {DEP: "partmod"}])
	matcher.add_pattern(1, [{LOWER: "leave"}, {DEP: "quantmod"}])
	matcher.add_pattern(1, [{LOWER: "leave"}, {ENT_TYPE: "TIME"}])
	matcher.add_pattern(1, [{LOWER: "leave"}, {LOWER: "in"}, {ENT_TYPE: "DATE"}])
	matcher.add_pattern(1, [{LOWER: "leave"}, {LOWER: "town"}])
	matcher.add_pattern(1, [{LOWER: "out"}, {LOWER: "of"}, {LOWER: "town"}])
	matcher.add_pattern(1, [{LOWER: "outta"}, {LOWER: "town"}])
	matcher.add_pattern(1, [{LOWER: "lastnight"}, {LOWER: "in"}, {LOWER: "town"}])
	matcher.add_pattern(1, [{LOWER: "back"}, {LOWER: "in"}, {LOWER: "town"}])
	matcher.add_pattern(1, [{LOWER: "just"}, {LOWER: "in"}, {LOWER: "town"}])
	matcher.add_pattern(1, [{LOWER: "day"}, {LOWER: "in"}, {LOWER: "town"}])
	matcher.add_pattern(1, [{LOWER: "in"}, {LOWER: "town"}, {LOWER: "tonight"}])
	matcher.add_pattern(1, [{LOWER: "in"}, {LOWER: "town"}, {LOWER: "through"}])
	matcher.add_pattern(1, [{LOWER: "in"}, {LOWER: "town"}, {LOWER: "until"}])
	matcher.add_pattern(1, [{LOWER: "in"}, {LOWER: "town"}, {LOWER: "for"}, {LOWER: "one"}, {LOWER: "night"}])
	matcher.add_pattern(1, [{LOWER: "in"}, {LOWER: "town"}, {LOWER: "for"}, {IS_DIGIT: True}, {LOWER: "night"}])
	matcher.add_pattern(1, [{LOWER: "town"}, {LOWER: "stay", DEP: "nmod"}])
	matcher.add_pattern(1, [{LOWER: "new"}, {LOWER: "girl"}, {LOWER: "in"}, {LOWER: "town"}])
	matcher.add_pattern(1, [{LOWER: "recent"}, {LOWER: "move"}])
	matcher.add_pattern(1, [{LOWER: "recently"}, {LOWER: "move"}])
	matcher.add_pattern(1, [{LOWER: "relocate"}])
	matcher.add_pattern(1, [{LOWER: "new", DEP: "amod"}, {LOWER: "city"}, {LOWER: "to", DEP: "dep"}])
	matcher.add_pattern(1, [{LOWER: "new"}, {LOWER: "to"}, {LOWER: "area"}])
	matcher.add_pattern(1, [{LOWER: "new"}, {LOWER: "to"}, {upper_start: True}])
	matcher.add_pattern(1, [{LOWER: "first"}, {LOWER: "visit"}, {LOWER: "to"}])
	matcher.add_pattern(1, [{LOWER: "i", DEP: "nsubj"}, {LOWER: "arrive"}])
	matcher.add_pattern(1, [{LOWER: "girl", DEP: "nsubj"}, {LOWER: "arrive"}, {DEP: "partmod"}])
	matcher.add_pattern(1, [{LOWER: "girl", DEP: "nsubj"}, {LOWER: "arrive"}, {DEP: "quantmod"}])
	matcher.add_pattern(1, [{LOWER: "just"}, {LOWER: "arrive"}])
	matcher.add_pattern(1, [{LOWER: "on"}, {LOWER: "my"}, {LOWER: "way"},{LOWER: "to"},{TAG: "NNP"}])
	matcher.add_pattern(1, [{LOWER: "on"}, {LOWER: "my"}, {LOWER: "way"},{LOWER: "to"},{TAG: "NN"}])
	matcher.add_pattern(1, [{LOWER: "on"}, {LOWER: "the"}, {LOWER: "way"}])
	matcher.add_pattern(1, [{LOWER: "just"}, {LOWER: "get"}, {LOWER: "here"}])
	matcher.add_pattern(1, [{LOWER: "get"}, {LOWER: "here"}, {LOWER: "today"}])
	matcher.add_pattern(1, [{LOWER: "get"}, {LOWER: "here"}, {LOWER: "yesterday"}])
	matcher.add_pattern(1, [{LOWER: "get"}, {LOWER: "here"}, {LOWER: "last"}, {LOWER: "night"}])
	matcher.add_pattern(1, [{LOWER: "i", DEP: "nsubj"}, {LOWER: "visit"}, {is_place: True, DEP: "dobj"}])

	# Strong Positive Matcher Patterns
	matcher.add_entity(2)
	matcher.add_pattern(2, [{LOWER: "new"}, {IS_ASCII: True}, {LOWER: "in"}, {is_place: True}])
	matcher.add_pattern(2, [{LOWER: "new"}, {IS_ASCII: True}, {IS_ASCII: True}, {LOWER: "in"}, {is_place: True}])
	matcher.add_pattern(2, [{LOWER: "im"}, {LOWER: "new"}, {LOWER: "in"}, {LOWER: "town"}])
	matcher.add_pattern(2, [{LOWER: "new"}, {LOWER: "in"}, {is_place: True}])
	matcher.add_pattern(2, [{LOWER: "new"}, {LOWER: "to"}, {is_place: True}])
	matcher.add_pattern(2, [{LOWER: "new"}, {is_girl: True}, {LOWER: "in"}, {LOWER: "town"}])
	matcher.add_pattern(2, [{LOWER: "new"}, {LOWER: "to"}, {upper_start: True}, {LOWER: "area"}])
	

	# Negative Matcher Patterns
	matcher.add_entity(3)
	matcher.add_pattern(3, [{LOWER: "new"}])
	matcher.add_pattern(3, [{LOWER: "girl"}, {LOWER: "in"}, {LOWER: "town"}])
	matcher.add_pattern(3, [{LOWER: "grand"}, {LOWER: "new"}])
	matcher.add_pattern(3, [{LOWER: "new"}, {LOWER: "at"}])
	matcher.add_pattern(3, [{LOWER: "new"}, {LOWER: "to"}, {LOWER: "business"}])
	matcher.add_pattern(3, [{LOWER: "new"}, {LOWER: "to"}, {LOWER: "industry"}])
	matcher.add_pattern(3, [{LOWER: "new"}, {LOWER: "to"}, {LOWER: "scenario"}])
	matcher.add_pattern(3, [{LOWER: "dream", DEP: "nsubj"}, {LOWER: "arrive"}])
	matcher.add_pattern(3, [{LOWER: "fantasy", DEP: "nsubj"}, {LOWER: "arrive"}])
	matcher.add_pattern(3, [{LOWER: "you", DEP: "nsubj"}, {LOWER: "arrive"}])
	matcher.add_pattern(3, [{LOWER: "area"}, {LOWER: "only"}])
	matcher.add_pattern(3, [{upper_start: True}, {LOWER: "area"}])
	matcher.add_pattern(3, [{LOWER: "you", DEP: "nsubj"}, {LOWER: "leave"}])
	matcher.add_pattern(3, [{LOWER: "it", DEP: "dobj"}, {LOWER: "leave"}, {IS_ASCII: True, DEP: "nmod"}])
	matcher.add_pattern(3, [{LOWER: "that", DEP: "dobj"}, {LOWER: "leave"}, {IS_ASCII: True, DEP: "nmod"}])
	matcher.add_pattern(3, [{LOWER: "best"}, {LOWER: "move"}])
	matcher.add_pattern(3, [{LOWER: "next"}, {LOWER: "move"}])
	matcher.add_pattern(3, [{LOWER: "arrive"}, {IS_ASCII: True, DEP: "xcomp"}])
	matcher.add_pattern(3, [{LOWER: "visit"}, {LOWER: "sister", DEP: "dobj"}])
	matcher.add_pattern(3, [{LOWER: "visit"}, {LOWER: "family", DEP: "dobj"}])
	matcher.add_pattern(3, [{LOWER: "we", DEP: "poss"}, {LOWER: "visit"}])

	# Strong Negative Matcher Patterns
	matcher.add_entity(4)
	matcher.add_pattern(4, [{LOWER: "town"}, {LOWER: "girl"}])
	matcher.add_pattern(4, [{LOWER: "on"}, {LOWER: "the"}, {LOWER: "town"}])
	matcher.add_pattern(4, [{LOWER: "near"}, {LOWER: "town"}])
	matcher.add_pattern(4, [{LOWER: "down"}, {LOWER: "town"}])
	matcher.add_pattern(4, [{LOWER: "town"}, {LOWER: "hall"}])
	matcher.add_pattern(4, [{LOWER: "best"}, {LOWER: "in"}, {LOWER: "town"}])
	matcher.add_pattern(4, [{LOWER: "best"}, {IS_ASCII: True}, {LOWER: "in"}, {LOWER: "town"}])
	matcher.add_pattern(4, [{LOWER: "best"}, {IS_ASCII: True}, {IS_ASCII: True}, {LOWER: "in"}, {LOWER: "town"}])
	matcher.add_pattern(4, [{LOWER: "best"}, {LOWER: "in"}, {IS_ASCII: True}, {LOWER: "town"}])
	matcher.add_pattern(4, [{LOWER: "best"}, {IS_ASCII: True}, {LOWER: "in"}, {IS_ASCII: True}, {LOWER: "town"}])
	matcher.add_pattern(4, [{LOWER: "best"}, {IS_ASCII: True}, {IS_ASCII: True}, {LOWER: "in"}, {IS_ASCII: True}, {LOWER: "town"}])
	matcher.add_pattern(4, [{LOWER: "not"}, {LOWER: "new"}, {LOWER: "in"}, {LOWER: "town"}])
	matcher.add_pattern(4, [{LOWER: "not"}, {LOWER: "new"}, {LOWER: "to"}, {LOWER: "town"}])
	matcher.add_pattern(4, [{LOWER: "not"}, {LOWER: "leave"}, {LOWER: "town"}])	
	matcher.add_pattern(4, [{LOWER: "i", DEP: "nsubj"}, {LOWER: "leave"}, {LOWER: "you", DEP: "dobj"}])
	matcher.add_pattern(4, [{LOWER: "new"}, {LOWER: "but"}])
	matcher.add_pattern(4, [{LOWER: "new"}, {LOWER: "backpage", DEP: "nmod"}])
	matcher.add_pattern(4, [{LOWER: "new"}, {LOWER: "bp", DEP: "nmod"}])
	matcher.add_pattern(4, [{LOWER: "new", DEP: "amod"}, {IS_ASCII: True}])
	matcher.add_pattern(4, [{LOWER: "new"}, {LOWER: "york"}])
	matcher.add_pattern(4, [{LOWER: "new"}, {LOWER: "delhi"}])
	#DS
	matcher.add_pattern(4, [{LOWER: "leave"}, {LOWER: "message", DEP: "dobj"}])
	matcher.add_pattern(4, [{LOWER: "leave"}, {LOWER: "msg", DEP: "dobj"}])
	matcher.add_pattern(4, [{LOWER: "leave"}, {LOWER: "txt", DEP: "dobj"}])
	matcher.add_pattern(4, [{LOWER: "leave"}, {LOWER: "text", DEP: "dobj"}])
	matcher.add_pattern(4, [{LOWER: "leave"}, {LOWER: "impression", DEP: "dobj"}])
	matcher.add_pattern(4, [{LOWER: "leave"}, {LOWER: "voicemail", DEP: "dobj"}])
	matcher.add_pattern(4, [{LOWER: "leave"}, {LOWER: "smile", DEP: "dobj"}])
	matcher.add_pattern(4, [{LOWER: "leave"}, {LOWER: "satisfied"}])
	matcher.add_pattern(4, [{LOWER: "leave"}, {LOWER: "memory", DEP: "dobj"}])
	matcher.add_pattern(4, [{LOWER: "leave"}, {LOWER: "you"}])
	matcher.add_pattern(4, [{LOWER: "leave"}, {LOWER: "u"}])
	matcher.add_pattern(4, [{LOWER: "leave"}, {LOWER: "with"}])
	matcher.add_pattern(4, [{LOWER: "leave"}, {LOWER: "a"}, {LOWER: "gentleman"}])
	matcher.add_pattern(4, [{LOWER: "or"}, {LOWER: "leave"}])
	matcher.add_pattern(4, [{LOWER: "or"}, {LOWER: "i"}, {LOWER: "leave"}])
	matcher.add_pattern(4, [{LOWER: "move"}, {LOWER: "on"}])
	matcher.add_pattern(4, [{LOWER: "i"}, {LOWER: "move"}, {LOWER: "like"}])
	matcher.add_pattern(4, [{LOWER: "arrive"}, {LOWER: "on"}, {LOWER: "time"}])
	matcher.add_pattern(4, [{LOWER: "can"}, {LOWER: "move"}])
	matcher.add_pattern(4, [{LOWER: "new"}, {LOWER: "but"}])
	matcher.add_pattern(4, [{LOWER: "on"}, {LOWER: "my"}, {LOWER: "way"}, {LOWER: "to"}, {TAG: "PRP"}])
	matcher.add_pattern(4, [{LOWER: "u"}, {LOWER: "get"}, {LOWER: "here"}])
	matcher.add_pattern(4, [{LOWER: "you"}, {LOWER: "get"}, {LOWER: "here"}])
	matcher.add_pattern(4, [{LOWER: "go"}, {LOWER: "to"}, {LOWER: "town"}])
	matcher.add_pattern(4, [{LOWER: "new"}, {LOWER: "management"}])

	return matcher

def post_process(matches, nlp_doc):
	movement = dict()
	for ent_id, label, start, end in matches:
		movement[str(nlp_doc[start:end])]={'start': start, 'end': end, 'ent_id': ent_id,}
	return movement

def wrap_value_with_context(movement):
	label_list=dict()
	label_list[1]="positive"
	label_list[2]="strong positive"
	label_list[3]="negative"
	label_list[4]="strong negative"
	result = []
	for element in movement:
		this_result = {
				"context": {
						"start": movement[element]['start'], 
						"end": movement[element]['end']
				},
				"value": element,
				"metadata": {
						"confidence_level": label_list[movement[element]['ent_id']]
				}
		}
		result.append(this_result)
	return result


def extract(nlp_doc, matcher):
	movement_matches = matcher(nlp_doc)
	movement = post_process(movement_matches, nlp_doc)
	info_extracted = wrap_value_with_context(movement)

	return info_extracted

