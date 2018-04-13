from etk.etk_extraction import Extractable
"TODO: Why import OriginRecord but not used, probably you want to extend from this class instead of Extractable?"
from etk.origin_record import OriginRecord
from typing import List, Dict

from etk.origin_record import OriginRecord


class ExtractionProvenanceRecord(Extractable):
    """
    A Provenance Record containing details of Extraction Results history.
    """

    def __init__(self, id: int, json_path: str, method: str, start_char: str, end_char: str, confidence, _document=None,
                 parent_extraction_provenance: List[int] = None) -> None:

        Extractable.__init__(self)
        self.id = id
        self.origin_record = OriginRecord(json_path, start_char, end_char, _document)
        self.method = method
        self.extraction_confidence = confidence
        self._document = _document
        self._parent_extraction_provenance = parent_extraction_provenance

    @property
    def full_path(self) -> str:
        """
        Returns: The full path of a JSONPath match
        """
        "TODO: What is json_path here, when it this attribute get created"
        return self.json_path

    @property
    def parent_extraction_provenance(self):
        """
        Returns: the parent extraction provenance id.
        """
        return self._parent_extraction_provenance
