from typing import List
import jsonpath_rw
import spacy
from etk.tokenizer import Tokenizer
from etk.document import Document


class ETK(object):

    def __init__(self):
        self.parser = jsonpath_rw.parse
        self.default_tokenizer = Tokenizer(nlp=spacy.load('en_core_web_sm'))

    def create_document(self, doc: dict) -> Document:
        """
        Factory method to wrap input JSON docs in an ETK Document object.

        Args:
            doc (dict): a JSON object containing a document in CDR format.

        Returns: wrapped Document

        """
        return Document(self, doc, self.default_tokenizer)

    def load_glossary(self, file_path) -> List[str]:
        """
        A glossary is a text file, one entry per line.

        Args:
            file_path (str): path to a text file containing a glossary.

        Returns: List of the strings in the glossary.
        """
        #to-do: this should be a list, not a dict
        res = dict()
        with open(file_path) as fp:
            line = fp.readline().rstrip('\n')
            while line:
                res[line] = line
                line = fp.readline().rstrip('\n')
        return res

