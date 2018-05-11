import datetime
import hashlib
import json
from typing import Dict


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
    def create_doc_id_from_json(doc):
        """

        Args:
            doc:

        Returns:

        """
        return hashlib.sha256(json.dumps(doc, sort_keys=True).encode('utf-8')).hexdigest()