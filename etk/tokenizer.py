import spacy
import re
from spacy.tokenizer import Tokenizer as spacyTokenizer
from spacy.tokens import Token, Doc
from typing import List

months_dict = {
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
    "dec": 12,
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
        """
        Tokenize the given text, returning a spacy doc. Used for spacy rule extractor

        Args:
            text (string):
            keep_multi_space

        Returns: Doc

        """
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

        def is_ordinal(token):
            return token.orth_[-2:] in ['rd', 'st', 'th', 'nd']
        spacy_token.set_extension("is_ordinal", getter=is_ordinal)

        def is_month(token):
            return token.lower_ in months_dict.keys()
        spacy_token.set_extension("is_month", getter=is_month)

        def is_mixed(token):
            if not token.is_title and not token.is_lower and not token.is_upper:
                return True
            else:
                return False
        spacy_token.set_extension("is_mixed", getter=is_mixed)

        """Add custom methods"""
        """Add get_prefix method. RETURN length N prefix"""
        def n_prefix(token, n):
            return token.text[:n]
        spacy_token.set_extension("n_prefix", method=n_prefix)

        """Add get_suffix method. RETURN length N suffix"""
        def n_suffix(token, n):
            return token.text[-n:]
        spacy_token.set_extension("n_suffix", method=n_suffix)

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