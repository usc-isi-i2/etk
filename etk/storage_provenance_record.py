from etk.etk_extraction import Extractable
"TODO: Why import OriginRecord but not used, probably you want to extend from this class instead of Extractable?"
from etk.origin_record import OriginRecord
from typing import List, Dict

class StorageProvenanceRecord(Extractable):
    """
    An individual segment in a document.
    For now, it supports recording of JSONPath results, but we should consider extending
    to record segments within a text doc, e.g., by start and end char, or segments within
    a token list with start and end tokens.
    """

    def __init__(self, json_path: str, attribute: str, extraction_provenances: List[int] = None,
                 _document=None) -> None:

        Extractable.__init__(self)
        self.field = None
        self._destination = json_path + '.' + attribute
        self.provenance_record_id = extraction_provenances # will be assigned later_provenances
        self._document = _document
        self.doc_id = None

    @property
    def destination(self) -> str:
        """
        Returns: The full path of a JSONPath match
        """
        "TODO: What is json_path here, when it this attribute get created"
        return self.json_path

    @property
    def document(self):
        """
        Returns: the parent Document
        """
        return self._document

    @property
    def destination(self):
        """
        Returns: the parent Document
        """
        return self._destination