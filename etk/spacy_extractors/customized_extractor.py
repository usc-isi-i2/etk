import codecs
import json
import spacy
import copy
import itertools

FLAG_DICT = {
    "18": spacy.attrs.FLAG18, "19": spacy.attrs.FLAG19, 
    "20": spacy.attrs.FLAG20, "21": spacy.attrs.FLAG21,
    "22": spacy.attrs.FLAG22, "23": spacy.attrs.FLAG23,
    "24": spacy.attrs.FLAG24, "25": spacy.attrs.FLAG25,
    "26": spacy.attrs.FLAG26, "27": spacy.attrs.FLAG27,
    "28": spacy.attrs.FLAG28, "29": spacy.attrs.FLAG29,
    "30": spacy.attrs.FLAG30, "31": spacy.attrs.FLAG31,
    "32": spacy.attrs.FLAG32, "33": spacy.attrs.FLAG33,
    "34": spacy.attrs.FLAG34, "35": spacy.attrs.FLAG35,
    "36": spacy.attrs.FLAG36, "37": spacy.attrs.FLAG37,
    "38": spacy.attrs.FLAG38, "39": spacy.attrs.FLAG39,
    "40": spacy.attrs.FLAG40, "41": spacy.attrs.FLAG41,
    "42": spacy.attrs.FLAG42, "43": spacy.attrs.FLAG43,
    "44": spacy.attrs.FLAG44, "45": spacy.attrs.FLAG45,
    "46": spacy.attrs.FLAG46, "47": spacy.attrs.FLAG47,
    "48": spacy.attrs.FLAG48, "49": spacy.attrs.FLAG49,
    "50": spacy.attrs.FLAG50, "51": spacy.attrs.FLAG51,
    "52": spacy.attrs.FLAG52, "53": spacy.attrs.FLAG53,
    "54": spacy.attrs.FLAG54, "55": spacy.attrs.FLAG55,
    "56": spacy.attrs.FLAG56, "57": spacy.attrs.FLAG57,
    "58": spacy.attrs.FLAG58, "59": spacy.attrs.FLAG59,
    "60": spacy.attrs.FLAG60, "61": spacy.attrs.FLAG61,
    "62": spacy.attrs.FLAG62, "63": spacy.attrs.FLAG63,
}

POS_MAP = {
    "noun": "NOUN",
    "pronoun": "PROPN",
    "punctuation": "PUNCT",
    "proper noun": "PROPN",
    "determiner": "DET",
    "symbol": "SYM",
    "adjective": "ADJ",
    "conjunction": "CONJ",
    "verb": "VERB",
    "pre/post-position": "ADP",
    "adverb": "ADV",
    "particle": "PART",
    "interjection": "INTJ",
}

name_dict = {
                1: u'IS_ALPHA', 
                2: u'IS_ASCII', 
                3: u'IS_DIGIT', 
                4: u'IS_LOWER', 
                5: u'IS_PUNCT', 
                6: u'IS_SPACE', 
                7: u'IS_TITLE', 
                8: u'IS_UPPER', 
                9: u'LIKE_URL', 
                10: u'LIKE_NUM', 
                11: u'LIKE_EMAIL', 
                12: u'IS_STOP', 
                13: u'IS_OOV', 
                14: u'FLAG14', 
                15: u'FLAG15', 
                16: u'FLAG16', 
                17: u'FLAG17', 
                18: u'FLAG18', 
                19: u'FLAG19', 
                20: u'FLAG20', 
                21: u'FLAG21', 
                22: u'FLAG22', 
                23: u'FLAG23', 
                24: u'FLAG24', 
                25: u'FLAG25', 
                26: u'FLAG26', 
                27: u'FLAG27', 
                28: u'FLAG28', 
                29: u'FLAG29', 
                30: u'FLAG30', 
                31: u'FLAG31', 
                32: u'FLAG32', 
                33: u'FLAG33', 
                34: u'FLAG34', 
                35: u'FLAG35', 
                36: u'FLAG36', 
                37: u'FLAG37', 
                38: u'FLAG38', 
                39: u'FLAG39', 
                40: u'FLAG40', 
                41: u'FLAG41', 
                42: u'FLAG42', 
                43: u'FLAG43', 
                44: u'FLAG44', 
                45: u'FLAG45', 
                46: u'FLAG46', 
                47: u'FLAG47', 
                48: u'FLAG48', 
                49: u'FLAG49', 
                50: u'FLAG50', 
                51: u'FLAG51', 
                52: u'FLAG52', 
                53: u'FLAG53', 
                54: u'FLAG54', 
                55: u'FLAG55', 
                56: u'FLAG56', 
                57: u'FLAG57', 
                58: u'FLAG58', 
                59: u'FLAG59', 
                60: u'FLAG60', 
                61: u'FLAG61', 
                62: u'FLAG62', 
                63: u'FLAG63',
                64: u'ID', 
                65: u'ORTH', 
                66: u'LOWER', 
                67: u'NORM', 
                68: u'SHAPE', 
                69: u'PREFIX', 
                70: u'SUFFIX', 
                71: u'LENGTH', 
                72: u'CLUSTER', 
                73: u'LEMMA', 
                74: u'POS', 
                75: u'TAG', 
                76: u'DEP', 
                77: u'ENT_IOB', 
                78: u'ENT_TYPE', 
                79: u'HEAD', 
                80: u'SPACY', 
                81: u'PROB'
}

'''
Class Rule
'''
class Rule(object):
    # initial and load english vocab
    def __init__(self, nlp):
        self.nlp = nlp
        self.flag_reset_lst = dict()

    # initial macher
    def init_matcher(self):
        self.matcher = spacy.matcher.Matcher(self.nlp.vocab)

    # initial flag for each user specified pattern
    def init_flag(self):
        self.flagnum = 17
        if self.flag_reset_lst:
            for flag in self.flag_reset_lst:
                for lexeme in self.flag_reset_lst[flag]:
                    self.nlp.vocab[lexeme].set_flag(FLAG_DICT[str(flag)], False)
        self.flag_reset_lst = dict()

    # set a flag, add all cases of a word to vocab and set flag to true
    def set_flag(self, token_l):
        self.flagnum += 1
        self.flag_reset_lst[self.flagnum] = []
        for t in token_l:
            elst = map(''.join, itertools.product(*((c.upper(), c.lower()) for c in t)))
            for lexeme in elst:
                self.nlp.vocab[lexeme.decode('utf8')].set_flag(FLAG_DICT[str(self.flagnum)], True)
                self.flag_reset_lst[self.flagnum].append(lexeme.decode('utf8'))

    def prep(self):
        self.new_nlp = lambda tokens: self.nlp.tokenizer.tokens_from_list(tokens)

'''
Class Pattern
'''
class Pattern(object):

    # initial a Pattern
    def __init__(self, entity):
        self.entity = entity
        '''
        First list contains pattern to be matched.
        Second dict contains information about prefix, suffix, shape
        '''
        self.token_lst = [[], {}]

    # add a word token
    def add_word_token(self, d, flag):
        token_to_rule = []
        
        for this_token in create_word_token(d["token"], d["capitalization"], 
                                            d["length"], flag):
            token_to_rule = add_pos_totoken(d["part_of_speech"], 
                                            this_token, token_to_rule)
        # add prefix and suffix information to token information for filter
        token_inf = create_inf(d["prefix"], d["suffix"], 
                                not d["token"], d["is_in_output"])
        self.token_lst = add_token_tolist(self.token_lst, token_to_rule, 
                            d["is_required"], token_inf)

    # add a shape token
    def add_shape_token(self, d):
        token_to_rule = []
        for this_token in create_shape_token(d["shapes"]):
            token_to_rule = add_pos_totoken(d["part_of_speech"], 
                                            this_token, token_to_rule)
        token_inf = create_inf(d["prefix"], d["suffix"], 
                                True, d["is_in_output"])
        # add shape information to token inf for future filter
        if d["shapes"]:
            token_inf["shapes"] = d["shapes"]
        self.token_lst = add_token_tolist(self.token_lst, token_to_rule, 
                            d["is_required"], token_inf)
    # add a glossary token
    def add_glossary_token(self, token_d):
        pass
    
    # add a number token
    def add_number_token(self, token_d, flag):
        pass

    # add a punctuation token
    def add_punctuation_token(self, token_d, flag):
        if not token_d["token"]:
            this_token = {spacy.attrs.IS_PUNCT: True}
        elif len(token_d["token"]) == 1:
            this_token = {spacy.attrs.ORTH: token_d["token"][0]}
        else:
            this_token = {FLAG_DICT[str(flag)]: True}
        token_inf = create_inf("", "", 
                                False, token_d["is_in_output"])
        self.token_lst = add_token_tolist(self.token_lst, [this_token], 
                        token_d["is_required"], token_inf)
    
    # add a symbol token
    def add_symbol_token(self, token_d):
        pass


# Check if prefix matches
def check_prefix(s, prefix):
    if len(s) < len(prefix):
        return False
    elif s[:len(prefix)].lower() == prefix.lower():
        return True
    else:
        return False

# Check if suffix matches
def check_suffix(s, suffix):
    if suffix:
        if len(s) < len(suffix):
            return False
        elif s[-len(suffix):].lower() == suffix.lower():
            return True
        else:
            return False
    else:
        return True

def create_inf(p, s, a, is_in_output):
    label = {}
    if a:
        if p:
            label["prefix"] = p
        if s:
            label["suffix"] = s
    if is_in_output == "true":
        label["is_in_output"] = True
    else:
        label["is_in_output"] = False
    
    return label


# Add each token to list to be processed by matcher
def add_token_tolist(t_lst, token_l, flag, inf):
    result = []
    result_lst = []
    result_dict = t_lst[1]
    lst = t_lst[0]

    # If this token is not optional
    if flag == "true":
        if lst:
            c = -1
            for s_id, token in enumerate(token_l):
                new_inf = copy.deepcopy(inf)
                # add shape information
                if "shapes" in inf:
                    new_inf["shape"] = inf["shapes"][s_id]
                    del new_inf["shapes"]
                for idx, each_lst in enumerate(lst):
                    c += 1
                    each_copy = copy.deepcopy(each_lst)
                    each_copy.append(token)
                    result.append(each_copy)
                    if c not in result_dict:
                        result_dict[c] = copy.deepcopy(result_dict[idx])
                        result_dict[c].update({len(each_copy)-1: new_inf})
                    else:
                        result_dict[c].update({len(each_copy)-1: new_inf})
        else:
            c = -1
            for s_id, token in enumerate(token_l):
                new_inf = copy.deepcopy(inf)
                if "shapes" in inf:
                    new_inf["shape"] = inf["shapes"][s_id]
                    del new_inf["shapes"]
                c += 1
                result.append([token])
                if c not in result_dict:
                    result_dict[c] = {0: new_inf}
                else:
                    result_dict[c].update({0: new_inf})

    # If this token is optional
    else:
        if lst:
            c = -1
            for each_lst in lst:
                result.append(copy.deepcopy(each_lst))
                c += 1
            for idx, each_lst in enumerate(lst):
                for s_id, token in enumerate(token_l):
                    new_inf = copy.deepcopy(inf)
                    if "shapes" in inf:
                        new_inf["shape"] = inf["shapes"][s_id]
                        del new_inf["shapes"]
                    c += 1
                    if c not in result_dict:
                        result_dict[c] = copy.deepcopy(result_dict[idx])
                    each_copy = copy.deepcopy(each_lst)
                    each_copy.append(token)
                    result.append(each_copy)
                    result_dict[c].update({len(each_copy)-1: new_inf})
        else:
            result.append([])
            c = 0
            result_dict[c] = {}
            for s_id, token in enumerate(token_l):
                new_inf = copy.deepcopy(inf)
                if "shapes" in inf:
                    new_inf["shape"] = inf["shapes"][s_id]
                    del new_inf["shapes"]
                c += 1
                result.append([token])
                if c not in result_dict:
                    result_dict[c] = {0: new_inf}
                else:
                    result_dict[c].update({0: new_inf})
    
    result_lst.append(result)
    result_lst.append(copy.deepcopy(result_dict))
    return result_lst

# add pos to each dict of token to be matched
def add_pos_totoken(pos_l, this_token, token_to_rule):
    # user check some POS
    if pos_l:
        for pos in pos_l:
            this_token[spacy.attrs.POS] = POS_MAP[pos]
            token_to_rule.append(copy.deepcopy(this_token))
    # if user does not specify any specific POS
    else:
        token_to_rule.append(copy.deepcopy(this_token))
    
    return token_to_rule

# create word token according to user input
def create_word_token(word_l, capi_l, length_l, flag):
    # if user enter one word
    if len(word_l) == 1:
        token = {spacy.attrs.LOWER: word_l[0].lower()}
        token_l = speci_capi(token, capi_l, word_l)
    # if user does not enter any word
    elif not word_l:
        token = {spacy.attrs.IS_ALPHA: True}
        token_l = speci_capi(token, capi_l, word_l)
        if length_l:
            while spacy.attrs.LENGTH not in token_l[0]:
                token = token_l.pop(0)
                for length in length_l:
                    token[spacy.attrs.LENGTH] = length
                    token_l.append(copy.deepcopy(token))
    # if user enter multiple words, use flag set before
    else:
        token = {FLAG_DICT[str(flag)]: True}
        token_l = speci_capi(token, capi_l, word_l)
    
    return token_l

# create shape token according to user input
def create_shape_token(shape_l):
    # if user does not enter a shape, it can be a number, punct
    if not shape_l:
        token = {spacy.attrs.IS_ASCII: True}
        token_l = [token]
    # if user enter some shape
    else:
        token_l = []
        for shape in shape_l:
            this_shape = generate_shape(shape, counting_stars(shape))
            token = {spacy.attrs.SHAPE: this_shape}
            token_l.append(copy.deepcopy(token))
    
    return token_l

# add capitalization to token
def speci_capi(t, capi_lst, word_l):
    result = []
    if not capi_lst:
        result.append(copy.deepcopy(t))
    else:
        if "exactly" in capi_lst and word_l != []:
            for word in word_l:
                token = copy.deepcopy(t)
                token[spacy.attrs.ORTH] = word
                result.append(token)
        if "lower" in capi_lst:
            token = copy.deepcopy(t)
            token[spacy.attrs.IS_LOWER] = True
            result.append(token)
        if "upper" in capi_lst:
            token = copy.deepcopy(t)
            token[spacy.attrs.IS_UPPER] = True
            result.append(token)
        if "title" in capi_lst:
            token = copy.deepcopy(t)
            token[spacy.attrs.IS_TITLE] = True
            result.append(token)
        if "mixed" in capi_lst:
            token = copy.deepcopy(t)
            token[spacy.attrs.IS_UPPER] = False
            token[spacy.attrs.IS_LOWER] = False
            token[spacy.attrs.IS_TITLE] = False
            result.append(token)
    
    return result

# Filter function is to process the output to match shape and prefix, suffix
def filter(doc, matches, inf_inf):
    result = []
    for match in matches:
        flag = True
        (ent_id, label, start, end) = match
        pattern = doc[start:end]
        for i in range(len(pattern)):
            inf = inf_inf[i]
            if "shape" in inf:
                if len(pattern[i]) != len(inf["shape"]):
                    flag = False
                    break
            if "prefix" in inf:
                prefix = inf["prefix"]
            else:
                prefix = ""
            if "suffix" in inf:
                suffix = inf["suffix"]
            else:
                suffix = ""
            if not check_prefix(str(pattern[i]), prefix) or not check_suffix(str(pattern[i]), suffix):
                flag = False
                break
        if flag:
            result.append(match)
    
    return result

# create output
def create_print(lst):
    result = []
    for each_d in lst:
        new_d = {}
        for e in each_d:
            new_d[name_dict[e].encode('utf-8')] = each_d[e]
        result.append(new_d)
    
    return result

def counting_stars(word):
    count = [1]
    for i in range(1,len(word)):
        if word[i-1] == word[i]:
            count[-1] += 1
        else:
            count.append(1)
    
    return count

def generate_shape(word, count):
    shape = ""
    p = 0
    for c in count:
        if c > 4:
            shape += word[p:p+4]
        else:
            shape += word[p:p+c]
        p = p + c
    
    return shape

def get_value(doc, start, end, output_inf):
    result_str = ""
    for i in range(len(output_inf)):
        if output_inf[i]:
            result_str += str(doc[start+i])
            result_str += " "
    return result_str.strip()            


def extract(field_rules, nlp_doc, nlp):

    #output_file = 'output.txt'
    #pattern_description = json.load(codecs.open(field_rules, 'r', 'utf-8'))

    pattern_description = field_rules
    #output_f = open(output_file, 'w')
    #doc = doc.decode('utf-8')

    rule = Rule(nlp)
    #rule_num = 0
    extracted_lst = []
    for index, line in enumerate(pattern_description["rules"]):
        rule.init_matcher()
        rule.init_flag()
        new_pattern = Pattern(index)

        for token_d in line["pattern"]:
            if token_d["type"] == "word":
                if len(token_d["token"]) >= 2:
                    # set flag for multiply words
                    rule.set_flag(token_d["token"])
                
                new_pattern.add_word_token(token_d, rule.flagnum)

            if token_d["type"] == "shape":
                new_pattern.add_shape_token(token_d)

            if token_d["type"] == "number":
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

        #rule.prep()
        
        nlp_doc = rule.nlp(nlp_doc)
        
        # print nlp_doc[1].lemma_
        # print nlp_doc[1].pos_
        # print nlp_doc[1].tag_
        # print nlp_doc[1].orth_
        # print nlp_doc[1].lower_
        # print nlp_doc[1].is_title
        # print nlp_doc[1].check_flag(spacy.attrs.FLAG18)
        # print nlp_doc[1].dep_

        tl = new_pattern.token_lst[0]
        ps_inf = new_pattern.token_lst[1]
        value_lst = []
        for i in range(len(tl)):
            #rule_num += 1
            if tl[i]:
                # rule_to_print = create_print(tl[i])
                # print rule_to_print
                rule.matcher.add_pattern(new_pattern.entity, tl[i], label = index)
                m = rule.matcher(nlp_doc)
                matches = filter(nlp_doc, m, ps_inf[i])

                output_inf = []
                for e in ps_inf[i]:
                    output_inf.append(ps_inf[i][e]["is_in_output"])

                
                for (ent_id, label, start, end) in matches:
                    value = get_value(nlp_doc, start, end, output_inf)
                    if value not in value_lst:
                        result = {
                            "value": value,
                            "context": {
                                "start": start,
                                "end": end,
                                "rule_id": label
                            }
                        }
                        extracted_lst.append(result)
                        value_lst.append(value)
#                   output_f.write(str(nlp_doc[start:end]))
#                   output_f.write("\n")
                rule.init_matcher()
    
    return extracted_lst
    #print json.dump(extracted_lst, indent=2)
    
#    print "total rule num:"
#    print rule_num

#   output_f.close()
