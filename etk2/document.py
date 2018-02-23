import json
from etk2.etk_extraction import Extractable
from etk2.segment import SegmentCollection, Segment
import jsonpath_rw


class Document(Extractable):
    """
        This class wraps raw CDR documents and provides a convenient API for ETK
        to query elements of the document and to update the document with the results
        of extractors.
        """

    def __init__(self, cdr_document, tokenizer=None) -> None:
        """
        Wrapper object for CDR documents.

        Args:
            cdr_document (JSON): the raw CDR document received in ETK.
            tokenizer (Tokenizer): the default tokenizer for creating tokens.

        Returns: the wrapped CDR document

        """
        Extractable.__init__(self)
        self.cdr_document = json.loads(cdr_document)
        self._value = self.cdr_document
        if tokenizer:
            self.tokenizer = tokenizer
        self.segments = None

    def select_segments(self, json_path) -> SegmentCollection or None:
        """
        Dereferences the json_path inside the document and returns the selected elements.
        This method should compile and cache the compiled json_path in case the same path
        is reused by multiple extractors.

        Args:
            json_path (str): a valid JSON path specification.

        Returns: A Segments object that contains the elements selected by the json_path.
        """
        path = jsonpath_rw.parse(json_path)
        matches = path.find(self.cdr_document)
        if not matches:
            return None
        else:
            segments = SegmentCollection()
            for a_match in matches:
                this_segment = Segment(str(a_match.full_path), a_match.value)
                segments.add(this_segment)

        return segments
