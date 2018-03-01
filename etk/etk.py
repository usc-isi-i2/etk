from typing import List, Dict
import jsonpath_rw
import spacy
from etk.tokenizer import Tokenizer
from etk.document import Document


class ETK(object):

    def __init__(self):
        self.parser = jsonpath_rw.parse
        self.default_tokenizer = Tokenizer(nlp=spacy.load('en_core_web_sm'))
        self.parsed = dict()

    def create_document(self, doc: Dict, mime_type: str=None, url: str="http://ex.com/123") -> Document:
        """
        Factory method to wrap input JSON docs in an ETK Document object.

        Args:
            doc (object): a JSON object containing a document in CDR format.
            mime_type (str): if doc is a tring, the mime_type tells what it is
            url (str): if the doc came from the web, specifies the URL for it

        Returns: wrapped Document

        """
        return Document(self, doc)

    def invoke_parser(self, jsonpath):
        if jsonpath not in self.parsed:
            self.parsed[jsonpath] = self.parser(jsonpath)

        return self.parsed[jsonpath]

    @staticmethod
    def load_glossary(file_path: str) -> List[str]:
        """
        A glossary is a text file, one entry per line.

        Args:
            file_path (str): path to a text file containing a glossary.

        Returns: List of the strings in the glossary.
        """
        with open(file_path) as fp:
            return fp.read().splitlines()

