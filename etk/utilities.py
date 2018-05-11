import datetime
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