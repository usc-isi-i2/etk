import datetime
import hashlib
import json
from typing import Dict
import uuid


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
    def create_uuid():
        return str(uuid.uuid4())
