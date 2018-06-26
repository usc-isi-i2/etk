from etk.extraction import Extractable
from typing import List, Dict

from etk.origin_record import OriginRecord


class ExtractionProvenanceRecord(Extractable):
    """
    A Provenance Record containing details of Extraction Results history.
    """

    def __init__(self, id: int, json_path: str, method: str, start_char: str, end_char: str, confidence, _document,
                 parent_extraction_provenance: int = None) -> None:

        Extractable.__init__(self)
        self.id = id
        self.origin_record = OriginRecord(json_path, start_char, end_char, _document)
        self.method = method
        self.extraction_confidence = confidence
        self._document = _document
        self._parent_extraction_provenance = parent_extraction_provenance

    @property
    def parent_extraction_provenance(self):
        """
        Returns: the parent extraction provenance id.
        """
        return self._parent_extraction_provenance

    def get_origins(self, value):
        if self._parent_extraction_provenance is None:
            return [self.origin_record]
        else:
            parent_id = self._parent_extraction_provenance
            parent = self._document.provenances[parent_id]
            origins = parent.get_origins(value)
            return origins
