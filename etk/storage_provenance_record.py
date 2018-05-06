from etk.extraction import Extractable
from typing import List, Dict


class StorageProvenanceRecord(Extractable):
    """
    An individual storage provenance record of a storage of set of a set of extraction results from a document.
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