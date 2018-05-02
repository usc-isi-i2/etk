from typing import Dict
from etk.knowledge_graph_schema import KGSchema
from etk.field_types import FieldType
from etk.etk_exceptions import KgValueError, ISODateError
from datetime import date, datetime
from etk.knowledge_graph_provenance_record import KnowledgeGraphProvenanceRecord
import numbers


class KnowledgeGraph(object):
    """
    This class is a knowledge graph object, provides API for user to construct their kg.
    Add field and value to the kg object, analysis on provenence
    """

    def __init__(self, schema: KGSchema, doc) -> None:
        self._kg = {}
        self.origin_doc = doc
        self.schema = schema

    def add_doc_value(self, field_name: str, jsonpath: str) -> None:
        """
        Add a value to knowledge graph by giving a jsonpath

        Args:
            field_name: str
            jsonpath: str

        Returns:
        """
        if self.schema.has_field(field_name):
            if field_name not in self._kg:
                self._kg[field_name] = []
            path = self.origin_doc.etk.parse_json_path(jsonpath)
            matches = path.find(self.origin_doc.value)

            all_valid = True
            for a_match in matches:
                if self.schema.is_valid(field_name, a_match.value):
                    
                    this_value = self.value_pre_process(a_match.value, field_name)
                    if {
                        "value": this_value,
                        "key": self.create_key_from_value(this_value, field_name)
                    } not in self._kg[field_name]:
                        self._kg[field_name].append({
                            "value": this_value,
                            "key": self.create_key_from_value(this_value, field_name)
                        })
                        self.create_kg_provenance("storage_location", str(this_value), str(a_match.full_path))

                else:
                    all_valid = False

            if not all_valid:
                raise KgValueError("Some kg value type invalid according to schema")

    def add_value(self, field_name: str, value, json_path_extraction: str = None) -> None:
        """
        Add a value to knowledge graph by giving a jsonpath

        Args:
            field_name: str
            value:

        Returns:
        """
        if field_name not in self._kg:
            self._kg[field_name] = []
        if self.schema.is_valid(field_name, value):
            value = self.value_pre_process(value, field_name)
            if {
                "value": value,
                "key": self.create_key_from_value(value, field_name)
            } not in self._kg[field_name]:
                self._kg[field_name].append({
                    "value": value,
                    "key": self.create_key_from_value(value, field_name)
                })
                if json_path_extraction != None:
                    self.create_kg_provenance("extraction_location", str(value), str(json_path_extraction))
        elif isinstance(value, list):
            all_valid = True
            for a_value in value:
                if self.schema.is_valid(field_name, a_value):
                    a_value = self.value_pre_process(a_value, field_name)
                    if {
                        "value": a_value,
                        "key": self.create_key_from_value(a_value, field_name)
                    } not in self._kg[field_name]:
                        self._kg[field_name].append({
                            "value": a_value,
                            "key": self.create_key_from_value(a_value, field_name)
                        })
                        if json_path_extraction != None:
                            self.create_kg_provenance("extraction_location", str(a_value), str(json_path_extraction))
                else:
                    all_valid = False
            if not all_valid:
                raise KgValueError("Some kg value type invalid according to schema")
        else:
            raise KgValueError("Invalid type of kg value: " + str(type(value) + " according to schema"))

    @property
    def value(self) -> Dict:
        """
        Get knowledge graph object

        Args:

        Returns: Dict
        """
        return self._kg

    def value_pre_process(self, v, field_name):
        """
        Pre process value

        Args:
            v: value
            field_name: str

        Returns: v
        """
        result = v
        if isinstance(result, str):
            result = result.strip()

        if self.schema.field_type(field_name) == FieldType.DATE:
            result = self.iso_date(result)

        return result

    @staticmethod
    def iso_date(d) -> str:
        """
        Return iso format of a date

        Args:
            d:
        Returns: str

        """
        if isinstance(d, datetime):
            return d.isoformat()
        elif isinstance(d, date):
            return datetime.combine(d, datetime.min.time()).isoformat()
        else:
            try:
                datetime.strptime(d, '%Y-%m-%dT%H:%M:%S')
                return d
            except ValueError:
                try:
                    datetime.strptime(d, '%Y-%m-%d')
                    return d + "T00:00:00"
                except ValueError:
                    pass
        raise ISODateError("Can not convert value to ISO format for kg")

    def create_key_from_value(self, value, field_name):
        """

        Args:
            value:
            field_name: str
        Returns: key

        """
        key = value
        if (isinstance(key, str) or isinstance(key, numbers.Number)) and self.schema.field_type(
                field_name) != FieldType.DATE:
            # try except block because unicode characters will not be lowered
            try:
                key = str(key).strip().lower()
            except:
                pass

        return key

    def create_kg_provenance(self, reference_type, value, json_path) -> None:
        kg_provenance_record: KnowledgeGraphProvenanceRecord = KnowledgeGraphProvenanceRecord("kg_provenance_record", reference_type, value, json_path, None)
        #if "provenances" not in self.cdr_document:
        #    self.cdr_document["provenances"] = {}
        #if "kg_provenances" not in self.cdr_document["provenances"]:
        #    self.cdr_document["provenances"]["kg_provenances"] = []
        if "provenances" not in self.origin_doc.cdr_document:
            self.origin_doc.cdr_document["provenances"] = []
        _dict = self.get_dict_kg_provenance(kg_provenance_record)
        self.origin_doc.cdr_document["provenances"].append(_dict)

    def get_dict_kg_provenance(self, kg_provenance_record: KnowledgeGraphProvenanceRecord):
        _dict = {}
        _dict["@type"] = kg_provenance_record._type
        _dict["reference_type"] = kg_provenance_record.reference_type
        _dict["value"] = kg_provenance_record._value
        _dict["json_path"] = kg_provenance_record.json_path
        return _dict
