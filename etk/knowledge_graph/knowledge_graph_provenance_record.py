from etk.extraction import Extractable
from etk.origin_record import OriginRecord


class KnowledgeGraphProvenanceRecord(Extractable):
    """
    A Provenance Record containing details of Extraction Results history.
    """

    def __init__(self, _id, _type: str, reference_type: str, _value: str, json_path: str, doc) -> None:

        Extractable.__init__(self)
        self.id = _id
        self._type = _type
        self._value = _value
        self.json_path = json_path
        self.reference_type = reference_type
        self.origin_doc = doc

    def get_origins(self, value):
        if self.reference_type == 'constant':
            return []
        else:
            origins = []
            break_point = self.json_path.rfind('.')
            if break_point == -1:
                json_path = self.json_path
            else:
                json_path = self.json_path[0:break_point]
            if json_path in self.origin_doc._jsonpath_provenances:
                matching_provenance_ids = self.origin_doc._jsonpath_provenances[json_path]
                for matched_prov_id in matching_provenance_ids:
                    prov = self.origin_doc._provenances[matched_prov_id]
                    origins.extend(prov.get_origins(value))
            else:
                origin_record = OriginRecord(json_path, None, None, self.origin_doc)
                origins.extend(origin_record)
            return origins
