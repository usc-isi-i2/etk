#!/usr/bin/env python
from ngram import ngram
import re
import unicodedata
import itertools

class RowTokenizer:
    def __init__(self, row, json_config):
        self.config = json_config
        self.settings = json_config["settings"]
        self.rows = []
        self.__tokenize(row)
        self.index = 0

    def next(self):
        if self.index < len(self.rows):
            self.index += 1
            return self.rows[self.index-1]
        return None

    def __tokenize(self, row):
        dict_analyzer = dict()
        dict_prefix = dict()
        dict_blank_fields = dict()

        final_row = list()

        for index, field_value in enumerate(row):
            dict_blank_fields[index] = False
            dict_prefix[index] = ""

            if str(index) in self.config["fieldConfig"]:
                field_config = self.config["fieldConfig"][str(index)]
                if "analyzer" in field_config:
                    analyzer = field_config["analyzer"]
                else:
                    analyzer = self.config["defaultConfig"]["analyzer"]

                if "prefix" in field_config:
                    dict_prefix[index] = field_config["prefix"]
                if "allow_blank" in field_config:
                    dict_blank_fields[index] = field_config["allow_blank"]
                final_row.append(field_value)
            else:
                final_row.append('')
                analyzer = self.config["defaultConfig"]["analyzer"]
            dict_analyzer[index] = analyzer

        row = final_row
        #print "Tokenize row:", row
        multi_lines = self.__get_cross_product(dict_blank_fields, row)
        for line in multi_lines:
            tokens = []
            for index, field_value in enumerate(line):
                field_tokens = self.__analyze_field(field_value,
                                                    dict_prefix[index],
                                                    dict_analyzer[index],
                                                    self.settings)
                tokens.extend(field_tokens)
            self.rows.append(tokens)


    @staticmethod
    def __get_cross_product(dict_blank_fields, line):
        field_values = []
        for index, field_value in enumerate(line):
            values = list()
            values.append(field_value)
            if len(field_value) > 0:
                if dict_blank_fields[index] is True:
                    values.append("")
            field_values.append(values)

        return itertools.product(*field_values)

    #returns the tokens removing the stop words
    @staticmethod
    def __tokenize_input_stopwords(input, stop_words):
        tokens = input.split()
        for token in tokens:
            if not token in stop_words:
                yield token

    @staticmethod
    def __tokenize_input(input):
        tokens = input.split()
        for token in tokens:
            yield token

    def __get_n_grams(self, text, n_type, n):
        #removes the stop words
        tokens = list(self.__tokenize_input(text))
        if n_type == "word":
            if len(tokens) > n:
                return ["".join(j) for j in zip(*[tokens[i:] for i in range(n)])]
            else:
                #returns the word directly if n is greater than number of words
                a = list()
                a.append(text)
                return a
        if n_type == "character":
            gram_object = ngram.NGram(N=n)
            gram_char_tokens = list(gram_object.split(text))
            if len(text) > n:
                return gram_char_tokens
            else:
                a = list()
                a.append(text)
                return a
        else:
            return list(self.__tokenize_input(text))

    # does regex evaluations specified in configuration file, converts to utf8, lowercase
    #returns the tokens character or word
    def __analyze_field(self, text, prefix, analyzer, settings):
        text = unicode(text)

        if "filters" in analyzer:
            for filter_name in analyzer["filters"]:
                if filter_name == "lowercase":
                    text = text.lower()
                elif filter_name == "uppercase":
                    text = text.upper()
                elif filter_name == "latin":
                    nfkd_form = unicodedata.normalize('NFKD', unicode(text))
                    text = unicode(nfkd_form)
                elif filter_name == 'mostlyHTML':
                    htmltags_count = len(re.findall("<.*?>",text))
                    if htmltags_count > 40:
                        text = ''
                else:
                    filter_settings = settings[filter_name]
                    if filter_settings["type"] == "stop":
                        words = filter_settings["words"]
                        tokens = self.__tokenize_input_stopwords(text, words)
                        text = unicode(" ".join(tokens))

        #evaluates based on the sent_replacements and word_replacements
        if "replacements" in analyzer:
            text = text.lower()
            wordlist = []
            if 'word_replacements' in analyzer['replacements']:
                for word in text.split():
                    word = word.replace('\\n','').replace('\\t','')
                    #we shouldn't remove the word that have \u in them those are unicode characters
                    if '\u' in word:
                        wordlist.append(word)
                        continue
                    if 'href' in word or 'www' in word:
                        continue
                    for replacement in analyzer['replacements']['word_replacements']:
                        p = re.compile(replacement['regex'], re.UNICODE)
                        word = p.sub(replacement['replacement'], word)
                    wordlist.append(word)
                    text = " ".join(wordlist)
            if 'sent_replacements' in analyzer['replacements']:
                for replacement in analyzer['replacements']["sent_replacements"]:
                    p = re.compile(replacement['regex'], re.UNICODE)
                    text = p.sub(replacement['replacement'], text)
                text = text.strip()
            elif 'word_replacements' not in analyzer['replacements'] or 'sent_replacements' not in analyzer['replacements']:
                for replacement in analyzer["replacements"]:
                    p = re.compile(replacement['regex'], re.UNICODE)
                    text = p.sub(replacement['replacement'], text)

        tokens = []
        if "tokenizers" in analyzer:
            for tokenizer in analyzer["tokenizers"]:
                if tokenizer == "whitespace":
                    tokens.extend(self.__tokenize_input(text))
                else:
                    tokenizer_setting = settings[tokenizer]
                    if tokenizer_setting["type"] == "character_ngram":
                        size = int(tokenizer_setting["size"])
                        tokens.extend(self.__get_n_grams(text, "character", size))
                    elif tokenizer_setting["type"] == "word_ngram":
                        size = int(tokenizer_setting["size"])
                        tokens.extend(self.__get_n_grams(text, "word", size))

        final_tokens = []

        for token in tokens:
            if len(token) > 0:
                final_tokens.append(prefix.encode('utf-8') + token.encode('utf-8'))
        return final_tokens
