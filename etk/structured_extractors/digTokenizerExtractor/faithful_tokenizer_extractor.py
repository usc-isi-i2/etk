# from digExtractor.extractor import Extractor
from digFaithfulTokenizer.faithful_tokenizer import FaithfulTokenizer
import copy


class Tokens(object):

    def __init__(self, tokens=None, reverse_map=None):
        self.tokens = tokens
        self.reverse_map = reverse_map

    def get_original_index(self, index):
        return self.reverse_map[index]


class FaithfulTokenizerExtractor(object):

    def __init__(self, setRecognizePunctuation=True, setRecognizeHtmlTags=True, setSkipHtmlEntities=True,
                 setRecognizeHtmlEntities=True, setSkipHtmlTags=True, recognize_linebreaks=True,
                 create_structured_tokens=False):
        tokenizer = FaithfulTokenizer(recognize_linebreaks=recognize_linebreaks,
                                      create_structured_tokens=create_structured_tokens)
        tokenizer.setRecognizePunctuation(setRecognizePunctuation)
        tokenizer.setRecognizeHtmlTags(setRecognizeHtmlTags)
        tokenizer.setSkipHtmlEntities(setSkipHtmlEntities)
        tokenizer.setRecognizeHtmlEntities(setRecognizeHtmlEntities)
        tokenizer.setSkipHtmlTags(setSkipHtmlTags)
        self.tokenizer = tokenizer
        self.metadata = {'extractor': 'tokenizer'}
        self.renamed_input_fields = 'text'

    def get_metadata(self):
        return copy.copy(self.metadata)

    def set_metadata(self, metadata):
        self.metadata = metadata
        return self

    def set_tokenizer(self, tokenizer):
        self.tokenizer = tokenizer
        return self

    def extract(self, text):
        self.faithful_tokens = self.tokenizer.tokenize(text)
        return self.faithful_tokens

    def get_renamed_input_fields(self):
        return self.renamed_input_fields

    def filter_tokens(self, config):
        # config contains the token types to be removed
        # [alphabet, digit, emoji, punctuation, html, html_entity, break]
        try:
            reverse_map = list()
            index = 0
            filtered_tokens = list()
            for token in self.faithful_tokens:
                if token['type'] not in config:
                    filtered_tokens.append(token)
                    reverse_map.append(index)
                index += 1

            return Tokens(filtered_tokens, reverse_map)

        except e:
            return None
