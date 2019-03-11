import datetime
import hashlib
import json
from typing import Dict
import uuid
import warnings


class Utility(object):

    @staticmethod
    def make_json_serializable(doc: Dict):
        """
        Make the document JSON serializable. This is a poor man's implementation that handles dates and nothing else.
        This method modifies the given document in place.

        Args:
            doc: A Python Dictionary, typically a CDR object.

        Returns: None

        """
        for k, v in doc.items():
            if isinstance(v, datetime.date):
                doc[k] = v.strftime("%Y-%m-%d")
            elif isinstance(v, datetime.datetime):
                doc[k] = v.isoformat()

    @staticmethod
    def create_doc_id_from_json(doc) -> str:
        """
        Docs with identical contents get the same ID.
        Args:
            doc:

        Returns: a string with the hash of the given document.

        """
        return hashlib.sha256(json.dumps(doc, sort_keys=True).encode('utf-8')).hexdigest()

    @staticmethod
    def create_doc_id_string(any_string):
        """
        Creates sha256 has of a string
        :param any_string: input string
        :return: sha256 hash of any_string
        """
        try:
            return hashlib.sha256(any_string).hexdigest()
        except:
            # probably failed because of unicode
            return hashlib.sha256(any_string.encode('utf-8')).hexdigest()

    @staticmethod
    def create_uuid():
        return str(uuid.uuid4())

    @staticmethod
    def create_description_from_json(doc_json):
        description = ''
        for key in doc_json:
            description += '"' + key + '":"' + str(doc_json[key]) + '", <br/>'
        description += '}'
        return description

def deprecated(msg=''):
    def deprecated_decorator(func):
        def deprecated_func(*args, **kwargs):
            warnings.warn("{}: this function is deprecated. {}".format(func.__name__, msg))
            return func(*args, **kwargs)
        return deprecated_func
    return deprecated_decorator
