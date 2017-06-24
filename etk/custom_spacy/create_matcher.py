import codecs
import json
import process_function


def custom_using_spacy(field_rule, doc, nlp):

	input_file = field_rule
	#output_file = 'output.txt'
	pattern_description = field_rule

	output_f = open(output_file, 'w')
	#doc = pattern_description["test_text"]

	rule = process_function.Rule(nlp)

	#rule_num = 0
	for index, line in enumerate(pattern_description["rules"]):
		rule.init_matcher()
		rule.init_flag()
		new_pattern = process_function.Pattern(index)

		for token_d in line["pattern"]:
			if token_d["type"] == "word":
				if len(token_d["token"]) >= 2:
					# set flag for multiply words
					rule.set_flag(token_d["token"])
				
				new_pattern.add_word_token(token_d, rule.flagnum)

			if token_d["type"] == "shape":
				new_pattern.add_shape_token(token_d)

			if token_d["type"] == "number":
				# if len(token_d["token"]) >= 2:
				# 	# set flag for multiply words
				# 	rule.set_flag(token_d["token"])
				new_pattern.add_number_token(token_d, rule.flagnum)

			if token_d["type"] == "punctuation":
				if len(token_d["token"]) >= 2:
					# set flag for multiply punctuation
					rule.set_flag(token_d["token"])
				
				new_pattern.add_punctuation_token(token_d, rule.flagnum)

			if token_d["type"] == "glossary":
				new_pattern.add_glossary_token(token_d)

			if token_d["type"] == "symbol":
				new_pattern.add_symbol_token(token_d)

		nlp_doc = rule.nlp(doc.decode('utf-8'))
		tl = new_pattern.token_lst[0]
		ps_inf = new_pattern.token_lst[1]
		
		for i in range(len(tl)):
	#		rule_num += 1
			if tl[i]:
				rule_to_print = process_function.create_print(tl[i])
				
				rule.matcher.add_pattern(new_pattern.entity, tl[i], label = 1)
				m = rule.matcher(nlp_doc)
				matches = process_function.filter(nlp_doc, m, ps_inf[i])
				
				for (ent_id, label, start, end) in matches:
					output_f.write(str(nlp_doc[start:end]))
					output_f.write("\n")
				rule.init_matcher()

	#print "total rule num:"
	#print rule_num

	output_f.close()
