from etk.etk_extraction import Extractable, Extraction
import copy
from typing import List, Dict


class Segment(Extractable):
    """
    An individual segment in a document.
    For now, it supports recording of JSONPath results, but we should consider extending
    to record segments within a text doc, e.g., by start and end char, or segments within
    a token list with start and end tokens.
    """
    def __init__(self, json_path: str, _value: Dict) -> None:
        Extractable.__init__(self)
        self.json_path = json_path
        self._value = _value
        self._extractions = []

    @property
    def full_path(self) -> str:
        """
        Returns: The full path of a JSONPath match
        """
        return self.json_path

    @property
    def document(self):
        """
        Still thinking about this, having the parent doc inside each extractable is convenient to avoid
        passing around the parent doc to all methods.

        Returns: the parent Document
        """
        pass

    def store_extractions(self, extractions: List[Extraction], attribute: str) -> None:
        """
        Records extractions in the container, and for each individual extraction inserts a
        ProvenanceRecord to record where the extraction is stored.
        Records the "output_segment" in the provenance.

        Extractions are always recorded in a list.

        Errors out if the segment is primitive, such as a string.

        Args:
            extractions (List[Extraction]):
            attribute (str): where to store the extractions.

        Returns:

        """
        self._extractions = (attribute, extractions)
        try:
            self._value[attribute] = [copy.deepcopy(a_extraction.value) for a_extraction in extractions]
        except Exception as e:
            print("segment is " + str(type(self._value)))
            print(e)

    @property
    def extractions(self) -> List[Extraction]:
        """
        Get the extractions stored in this container.
        Returns: List[Extraction]

        """
        return self._extractions
