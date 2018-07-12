from etk.extraction import Extractable
from typing import List, Dict
from etk.origin_record import OriginRecord


class StorageProvenanceRecord(Extractable):
    """
    An individual storage provenance record of a storage of set of a set of extraction results from a document.
    """

    def __init__(self, id, json_path: str, attribute: str, extraction_provenances,
                 _document=None) -> None:

        Extractable.__init__(self)
        self.id = id
        self.field = None
        self._destination = json_path + '.' + attribute
        self.extraction_provenances = extraction_provenances  # will be assigned later_provenances
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

    def get_origins(self, value):

        prov_ids = self.extraction_provenances
        if value in prov_ids:
            extraction_prov_id = prov_ids[value]
            extraction_prov = self._document.provenances[extraction_prov_id]
            origins = extraction_prov.get_origins(value)
            return origins
        else:
            originRecord = OriginRecord(self._destination, None, None, self._document)
            return [originRecord]
