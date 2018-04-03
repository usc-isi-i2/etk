from etk.etk_extraction import Extractable, Extraction
from typing import List, Dict
from etk.exception import StoreExtractionError


class Segment(Extractable):
    """
    An individual segment in a document.
    For now, it supports recording of JSONPath results, but we should consider extending
    to record segments within a text doc, e.g., by start and end char, or segments within
    a token list with start and end tokens.
    """
    def __init__(self, json_path: str, _value: Dict, _document=None) -> None:
        Extractable.__init__(self)
        self.json_path = json_path
        self._value = _value
        self._extractions = dict()
        self._document = _document

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
        return self._document

    def store_extractions(self, extractions: List[Extraction], attribute: str, group_by_tags: bool=True) -> None:
        """
        Records extractions in the container, and for each individual extraction inserts a
        ProvenanceRecord to record where the extraction is stored.
        Records the "output_segment" in the provenance.

        Extractions are always recorded in a list.

        Errors out if the segment is primitive, such as a string.

        Args:
            extractions (List[Extraction]):
            attribute (str): where to store the extractions.
            group_by_tags (bool): Set to True to use tags as sub-keys, and values of Extractions
                with the same tag will be stored in a list as the value of the corresponding key.
                (if none of the Extractions has 'tag', do not group by tags)

        Returns:

        """
        if not isinstance(self._value, dict):
            raise StoreExtractionError("segment is type: " + str(type(self._value)))

        if not len(extractions):
            return

        if group_by_tags:
            try:
                next(x for x in extractions if x.tag)   # if there is at least one extraction with a tag
                if attribute not in self._extractions:
                    self._extractions[attribute] = set([])
                    self._value[attribute] = {}
                for e in extractions:
                    tag = e.tag if e.tag else 'NO_TAGS'
                    if tag not in self.value[attribute]:
                        self.value[attribute][tag] = [e.value]
                    else:
                        if e.value not in self.value[attribute][tag]:
                            self.value[attribute][tag].append(e.value)
                self._extractions[attribute] = self._extractions[attribute].union(extractions)
                return
            except StopIteration:
                pass

        if attribute not in self._extractions:
            self._extractions[attribute] = set([])
            self._value[attribute] = []

        self._extractions[attribute] = self._extractions[attribute].union(extractions)

        for a_extraction in extractions:
            if a_extraction.value not in self._value[attribute]:
                self._value[attribute].append(a_extraction.value)

    @property
    def extractions(self) -> Dict:
        """
        Get the extractions stored in this container.
        Returns: Dict

        """
        return self._extractions
