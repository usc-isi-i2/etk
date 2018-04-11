from etk.etk_extraction import Extractable, Extraction
from typing import List, Dict
from etk.etk_exceptions import StoreExtractionError
from etk.storage_provenance_record import StorageProvenanceRecord

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
				provenance_ids = []
                for e in extractions:
                    tag = e.tag if e.tag else 'NO_TAGS'
                    if tag not in self.value[attribute]:
                        self.value[attribute][tag] = [e.value]
                    else:
                        if e.value not in self.value[attribute][tag]:
                            self.value[attribute][tag].append(e.value)
					provenance_ids.append(e.prov_id)
                self._extractions[attribute] = self._extractions[attribute].union(extractions)
				storage_provenance_record: StorageProvenanceRecord = StorageProvenanceRecord(self.json_path, attribute, provenance_ids, self.document)
                self.create_provenance(storage_provenance_record)
                return
            except StopIteration:
                pass

        if attribute not in self._extractions:
            self._extractions[attribute] = set([])
            self._value[attribute] = []

        self._extractions[attribute] = self._extractions[attribute].union(extractions)
		provenance_ids = []
        for a_extraction in extractions:
			provenance_ids.append(a_extraction.prov_id)
            if a_extraction.value not in self._value[attribute]:
                self._value[attribute].append(a_extraction.value)
		storage_provenance_record: StorageProvenanceRecord = StorageProvenanceRecord(self.json_path, attribute, provenance_ids, self.document)
        self.create_provenance(storage_provenance_record)

    @property
    def extractions(self) -> Dict:
        """
        Get the extractions stored in this container.
        Returns: Dict

        """
        return self._extractions


    def create_provenance(self, storage_provenance_record: StorageProvenanceRecord) -> None:
        if "provenances" not in self.document.cdr_document:
            self.document.cdr_document["provenances"] = []
        self.document.cdr_document["provenances"].append(self.get_dict_storage_provenance(storage_provenance_record))


    def get_dict_storage_provenance(self, storage_provenance_record: StorageProvenanceRecord) -> None:
        dict = {}
        dict["@type"] = "storage_provenance_record"
        dict["doc_id"] = storage_provenance_record.doc_id
        dict["field"] = storage_provenance_record.field
        dict["destination"] = storage_provenance_record.destination
        dict["provenance_record_id"] = storage_provenance_record.provenance_record_id
        return dict
