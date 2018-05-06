from etk.extraction import Extractable
from typing import List, Dict


class KnowledgeGraphProvenanceRecord(Extractable):
    """
    A Provenance Record containing details of Extraction Results history.
    """

    def __init__(self, _type: str, reference_type:str, _value: str, json_path: str, extraction_storage_provenance_ids: List[int] = None) -> None:

        Extractable.__init__(self)
        self._type = _type
        self._value = _value
        self.json_path = json_path
        self.reference_type =reference_type
