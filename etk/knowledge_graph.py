from typing import Dict
import numbers
from etk.knowledge_graph_schema import KGSchema
from etk.field_types import FieldType
from etk.etk_exceptions import KgValueError
from etk.knowledge_graph_provenance_record import KnowledgeGraphProvenanceRecord
from etk.extraction import Extraction


class KnowledgeGraph(object):
    """
    This class is a knowledge graph object, provides API for user to construct their kg.
    Add field and value to the kg object, analysis on provenence
    """

    def __init__(self, schema: KGSchema, doc) -> None:
        self._kg = {}
        self.origin_doc = doc
        self.schema = schema

    def _add_single_value(self, field_name: str, value, provenance_path=None) -> bool:
        (valid, this_value) = self.schema.is_valid(field_name, value)
        if valid:
            if {
                "value": this_value,
                "key": self.create_key_from_value(this_value, field_name)
            } not in self._kg[field_name]:
                self._kg[field_name].append({
                    "value": this_value,
                    "key": self.create_key_from_value(this_value, field_name)
                })
                if provenance_path:
                    self.create_kg_provenance("storage_location", str(this_value), provenance_path)
            return True
        else:
            return False

    def _add_value(self, field_name: str, value, provenance_path=None) -> bool:
        """
        Helper function to add values to a knowledge graph
        Args:
            field_name: a field in the knowledge graph, assumed correct
            value: any Python type

        Returns: True if the value is compliant with the field schema, False otherwise

        """
        if not isinstance(value, list):
            value = [value]

        all_valid = True
        for x in value:
            valid = self._add_single_value(field_name, x, provenance_path=provenance_path)
            all_valid = all_valid and valid
        return all_valid

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
                # If the value is the empty string, we treat is a None.
                if a_match.value:
                    valid = self._add_value(field_name, a_match.value, provenance_path=str(a_match.full_path))
                    all_valid = all_valid and valid

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

        (valid, value) = self.schema.is_valid(field_name, value)
        if valid:
            # The following code needs refactoring as it suffers from egregious copy/paste
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

                # Pedro added the following code for adding extraction
                if isinstance(a_value, Extraction):
                    self._add_single_value(field_name, a_value.value, provenance_path=str(json_path_extraction))

                # The following code needs refactoring as it suffers from egregious copy/paste
                else:
                    (valid, a_value) = self.schema.is_valid(field_name, a_value)
                    if valid:
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

    def create_key_from_value(self, value, field_name):
        """

        Args:
            value:
            field_name: str
        Returns: key

        """
        key = value
        if self.schema.field_type(field_name) == FieldType.KG_ID:
            pass
        elif (isinstance(key, str) or isinstance(key, numbers.Number)) and self.schema.field_type(
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
