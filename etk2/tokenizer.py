import spacy
import re
from spacy.tokenizer import Tokenizer as spacyTokenizer


class Tokenizer(object):
    """
    Abstract class used for all tokenizer implementations.
    """

    def tokenize(self, text):
        """
        Tokenizes the given text, returning a list of tokens.
        Or maybe should return an iterator, this is TBD.

        Args:
            text (string):

        Returns:

        """
        nlp = spacy.load('en_core_web_sm')

        """
        Customize tokenizer
        For future improvement, look at https://spacy.io/api/tokenizer, https://github.com/explosion/spaCy/issues/1494

        """
        prefix_re = re.compile(r'''^[\[\(\-\."']''')
        infix_re = re.compile(r'''[\@\-\(\)]|(?![0-9])\.(?![0-9])''')

        def custom_tokenizer(nlp):
            return spacyTokenizer(nlp.vocab, prefix_search=prefix_re.search, infix_finditer=infix_re.finditer)

        nlp.tokenizer = custom_tokenizer(nlp)
        # spacy_tokens = nlp(re.sub(' +', ' ', text))
        spacy_tokens = nlp(text)
        tokens = [self.custom_token(a_token) for a_token in spacy_tokens]
        return tokens

    @staticmethod
    def custom_token(spacy_token):
        """
        Function for token attributes extension, methods extension
        Use set_extension method.
        Reference: https://spacy.io/api/token, https://spacy.io/usage/processing-pipelines#custom-components-attributes

        """

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

        """To Do: is_integer, is_float, length, is_linkbreak, is_month, is_following_space?, is_followed_by_space?"""

        """Add get_prefix method. RETURN length N prefix"""
        def n_prefix(token, n):
            return token.text[:n]
        spacy_token.set_extension("N_prefix", method=n_prefix)

        """Add get_suffix method. RETURN length N suffix"""
        def n_suffix(token, n):
            return token.text[-n:]
        spacy_token.set_extension("N_suffix", method=n_suffix)

        """To Do: 
        1. Method convert_to_number: RETURN """

        return spacy_token

    def reconstruct_text(self, tokens):
        """
        Given a list of tokens, reconstruct the original text with as much fidelity as possible.

        Args:
            tokens ():

        Returns: a string.

        """
        return "".join([x.text_with_ws for x in tokens])