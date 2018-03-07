import spacy
import re
from spacy.tokenizer import Tokenizer as spacyTokenizer
from spacy.tokens import Token, Doc
from typing import List


class Tokenizer(object):
    """
    Abstract class used for all tokenizer implementations.
    """

    def __init__(self, nlp=spacy.load('en_core_web_sm')) -> None:
        """Load vocab, more vocab are available at: https://spacy.io/models/en"""
        self.nlp = nlp

        """Custom tokenizer"""
        self.nlp.tokenizer = self.custom_tokenizer()

    def tokenize(self, text: str, keep_multi_space: bool = False) -> List[Token]:
        """
        Tokenize the given text, returning a list of tokens. Type token: class spacy.tokens.Token

        Args:
            text (string):
            keep_multi_space

        Returns: [tokens]

        """
        """Tokenize text"""
        if not keep_multi_space:
            text = re.sub(' +', ' ', text)
        spacy_tokens = self.nlp(text)
        tokens = [self.custom_token(a_token) for a_token in spacy_tokens]

        return tokens

    def tokenize_to_spacy_doc(self, text: str, keep_multi_space: bool = False) -> Doc:
        if not keep_multi_space:
            text = re.sub(' +', ' ', text)
        doc = self.nlp(text)
        for a_token in doc:
            self.custom_token(a_token)

        return doc

    def custom_tokenizer(self) -> spacyTokenizer:
        """
        Custom tokenizer
        For future improvement, look at https://spacy.io/api/tokenizer, https://github.com/explosion/spaCy/issues/1494
        """
        prefix_re = re.compile(r'''^[\[()\-.,@#$%^&*?|<~+_:;>!"']''')
        infix_re = re.compile(r'''[\[()\-,@#$%^&*?|<~+_:;>!"']|(?![0-9])\.(?![0-9])|\n+ ''')
        return spacyTokenizer(self.nlp.vocab, rules=None, prefix_search=prefix_re.search, suffix_search=None,
                              infix_finditer=infix_re.finditer, token_match=None)

    @staticmethod
    def custom_token(spacy_token) -> Token:
        """
        Function for token attributes extension, methods extension
        Use set_extension method.
        Reference: https://spacy.io/api/token, https://spacy.io/usage/processing-pipelines#custom-components-attributes

        """

        """Add custom attributes"""
        """Add full_shape attribute. Eg. 21.33 => dd.dd, esadDeweD23 => xxxxXxxxXdd"""
        def get_shape(token):
            full_shape = ""
            for i in token.text:
                if i.isdigit():
                    full_shape += "d"
                elif i.islower():
                    full_shape += "x"
                elif i.isupper():
                    full_shape += "X"
                else:
                    full_shape += i
            return full_shape
        spacy_token.set_extension("full_shape", getter=get_shape)

        def is_integer(token):
            pattern = re.compile('^[-+]?[0-9]+$')
            return bool(pattern.match(token.text))
        spacy_token.set_extension("is_integer", getter=is_integer)

        def is_decimal(token):
            pattern = re.compile('^[-+]?[0-9]+\.[0-9]+$')
            return bool(pattern.match(token.text))
        spacy_token.set_extension("is_decimal", getter=is_decimal)

        # def is_linebreak(token):


        """To Do: 
            is_integer(boolean), 
            is_float(boolean), 
            is_linkbreak(\n) (boolean), 
            is_month(boolean), 
            is_mixed(eg.xXxX) (boolean), 
            is_alphanumeric(sda23d) (boolean), 
            is_following_space?(boolean), 
            is_followed_by_space?(boolean),
            is_space?(boolean)
        ...
        """

        """Add custom methods"""
        """Add get_prefix method. RETURN length N prefix"""
        def n_prefix(token, n):
            return token.text[:n]
        spacy_token.set_extension("n_prefix", method=n_prefix)

        """Add get_suffix method. RETURN length N suffix"""
        def n_suffix(token, n):
            return token.text[-n:]
        spacy_token.set_extension("n_suffix", method=n_suffix)

        """To Do: 
        1. Method convert_to_number: RETURN number, type integer if is integer, float if is float, else None
        2. Method find_substring(args): 
            args can be a string or a regex
            RETURN start index of first matches if exist, else None
        """

        return spacy_token

    @staticmethod
    def reconstruct_text(tokens: List[Token]) -> str:
        """
        Given a list of tokens, reconstruct the original text with as much fidelity as possible.

        Args:
            [tokens]:

        Returns: a string.

        """
        return "".join([x.text_with_ws for x in tokens])