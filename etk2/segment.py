from etk_extraction import Extractable, ExtractableCollection
import json


class Segment(Extractable):
    """
    An individual segment in a document.
    For now, it supports recording of JSONPath results, but we should consider extending
    to record segments within a text doc, e.g., by start and end char, or segments within
    a token list with start and end tokens.
    """
    def __init__(self, json_path, _value):
        Extractable.__init__(self)
        self.json_path = json_path
        self._value = _value
        self._extractions = None

    @property
    def full_path(self):
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

    def add_extractions(self, extractions, attribute):
        """
        Records extractions in the container, and for each individual extraction inserts a
        ProvenanceRecord to record where the extraction is stored.
        Records the "output_segment" in the provenance.

        Extractions are always recorded in a list.

        Errors out if the segment is primitive, such as a string.

        Args:
            extractions (ExtractionCollection):
            attribute (str): where to store the extractions.

        Returns:

        """
        self._extractions = extractions
        extractions_list = list()
        for a_extraction in extractions.items():
            extractions_list.append(a_extraction.value)
        if isinstance(self._value, dict):
            self._value[attribute] = extractions_list
        else:
            print("segment is "+str(type(self._value)))

    @property
    def extractions(self):
        """
        Get the extractions stored in this container.
        Returns: ExtractionCollection

        """
        return self._extractions


class SegmentCollection(ExtractableCollection):
    """
    Encapsulates a collection of segments that exist inside a Document.
    """
    def __init__(self):
        ExtractableCollection.__init__(self)
        self.collection_set = set([])
        self.collection_list = list()

    def add(self, segment):
        """
        Adds a new Segment

        Args:
            segment (Segment):
        """
        segment_str = json.dumps(segment.value)
        if segment_str not in self.collection_set:
            self.collection_set.add(segment_str)
            self.collection_list.append(segment)

    def union(self, segment_collection):
        """
        Update this collection to include all the segments passed
        Args:
            segment_collection: SegmentCollection:

        Returns: self, to allow chaining
        """
        for a_segment in segment_collection.collection_list:
            segment_str = json.dumps(a_segment.value)
            if segment_str not in self.collection_set:
                self.collection_set.add(segment_str)
                self.collection_list.append(a_segment)

    def items(self):
        """
        Returns: all the Segment objects as a Python list
        """
        return self.collection_list

    def all_values(self):
        """
        Convenience function.

        Returns: all values stored in all segments in the collection
        """
        return [x.value for x in self.collection_list]