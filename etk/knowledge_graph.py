from typing import Dict, List
import numbers
from etk.knowledge_graph_schema import KGSchema
from etk.field_types import FieldType
from etk.etk_exceptions import KgValueError, UndefinedFieldError
from etk.knowledge_graph_provenance_record import KnowledgeGraphProvenanceRecord
from etk.extraction import Extraction
from etk.segment import Segment
from etk.ontology_api import Ontology


class KnowledgeGraph(object):
    """
    This class is a knowledge graph object, provides API for user to construct their kg.
    Add field and value to the kg object, analysis on provenance
    """

    def __init__(self, schema: KGSchema, ontology: Ontology, doc) -> None:
        self._kg = {}
        self.origin_doc = doc
        self.schema = schema
        self.ontology = ontology
        if self.origin_doc.etk.generate_json_ld:
            if "doc_id" in doc.cdr_document:
                self.add_value("@id", self.origin_doc.cdr_document["doc_id"])

    def validate_field(self, field_name: str) -> bool:
        """
        Complain if a field in not in the schema
        Args:
            field_name:

        Returns: True if the field is present.

        """
        if field_name in {"@id", "@type"}:
            return True
        result = self.schema.has_field(field_name)
        if not result:
            # todo: how to comply with our error handling policies?
            raise UndefinedFieldError("'{}' should be present in the knowledge graph schema.".format(field_name))
        return result

    def _add_single_value(self, field_name: str, value, provenance_path=None,
                          reference_type="location") -> bool:
        if field_name == "@id":
            self._kg["@id"] = value
            return True
        if field_name == "@type" and self.origin_doc.etk.generate_json_ld:
            self._kg["@type"] = self._kg.get("@type", list())
            if value not in self._kg["@type"]:
                self._kg["@type"].append(value)
            return True
        (valid, this_value) = self.schema.is_valid(field_name, value)
        if self.ontology and self.origin_doc.etk.generate_json_ld:
            valid_value = valid and self.ontology.is_valid(field_name, this_value, self._kg)
        else:
            valid_value = valid and {'value': this_value, 'key': self.create_key_from_value(this_value, field_name)}
        if valid_value:
            if valid_value not in self._kg[field_name]:
                self._kg[field_name].append(valid_value)
            self.create_kg_provenance(reference_type, str(this_value), provenance_path) \
                if provenance_path else self.create_kg_provenance(reference_type, str(this_value))
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

    def _add_doc_value(self, field_name: str, jsonpath: str) -> None:
        """
        Add a value to knowledge graph by giving a jsonpath

        Args:
            field_name: str
            jsonpath: str

        Returns:
        """
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

    def add_value(self, field_name: str, value: object = None, json_path: str = None,
                  json_path_extraction: str = None, discard_empty: bool = True) -> None:
        """
        Add a value to knowledge graph.
        Input can either be a value or a json_path. If the input is json_path, the helper function _add_doc_value is
        called.
        If the input is a value, then it is handled

        Args:
            field_name: str, the field name in the knowledge graph
            value: the value to be added to the knowledge graph
            json_path: str, if json_path is provided, then get the value at this path in the doc
            json_path_extraction: str,
            discard_empty: bool,
        Returns:
        """
        self.validate_field(field_name)
        if field_name not in self._kg:
            self._kg[field_name] = []

        if json_path:
            self._add_doc_value(field_name, json_path)

        if value or (value is not None and not discard_empty):

            if not isinstance(value, list):
                value = [value]

            all_valid = True
            for a_value in value:
                if isinstance(a_value, Extraction):
                    valid = self._add_single_value(field_name, a_value.value, provenance_path=str(json_path_extraction))
                elif isinstance(a_value, Segment):
                    valid = self._add_single_value(field_name, a_value.value, provenance_path=a_value.json_path)
                else:
                    valid = self._add_single_value(field_name, a_value, provenance_path=json_path_extraction,
                                                   reference_type="constant")

                all_valid = all_valid and valid

            if not all_valid:
                print("Some kg value type invalid according to schema")
                # raise KgValueError("Some kg value type invalid according to schema")
        # IF we did not add any value, remove the empty field we just added to kg
        if len(self._kg[field_name]) == 0:
            self._kg.pop(field_name)

    @property
    def value(self) -> Dict:
        """
        Get knowledge graph object

        Args:

        Returns: Dict
        """
        return self._kg

    def get_values(self, field_name: str) -> List[object]:
        """
        Get a list of all the values of a field.

        Args:
            field_name:

        Returns: the list of values (not the keys)

        """
        result = list()
        if self.validate_field(field_name):
            for value_key in self._kg.get(field_name):
                result.append(value_key["value"])
        return result

    def create_key_from_value(self, value, field_name: str):
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

    def create_kg_provenance(self, reference_type, value, json_path: str = None) -> None:
        new_id = self.origin_doc.provenance_id_index
        kg_provenance_record: KnowledgeGraphProvenanceRecord = KnowledgeGraphProvenanceRecord(new_id,
                                                                                              "kg_provenance_record",
                                                                                              reference_type, value,
                                                                                              json_path,
                                                                                              self.origin_doc)
        self.origin_doc.provenance_id_index_incrementer()
        if value in self.origin_doc.kg_provenances:
            self.origin_doc.kg_provenances[value].append(new_id)
        else:
            self.origin_doc.kg_provenances[value] = [new_id]
        self.origin_doc.provenances[new_id] = kg_provenance_record
        if "provenances" not in self.origin_doc.cdr_document:
            self.origin_doc.cdr_document["provenances"] = []
        _dict = self.get_dict_kg_provenance(kg_provenance_record)
        self.origin_doc.cdr_document["provenances"].append(_dict)

    @staticmethod
    def get_dict_kg_provenance(kg_provenance_record: KnowledgeGraphProvenanceRecord):
        _dict = dict()
        _dict["@id"] = kg_provenance_record.id
        _dict["@type"] = kg_provenance_record._type
        _dict["reference_type"] = kg_provenance_record.reference_type
        _dict["value"] = kg_provenance_record._value
        if kg_provenance_record.json_path is not None:
            _dict["json_path"] = kg_provenance_record.json_path
        return _dict

    def context_resolve(self, field_uri: str) -> str:
        """
        According to field_uri to add corresponding context and return a resolvable field_name

        :param field_uri:
        :return: a field_name that can be resolved with kg's @context
        """
        from rdflib.namespace import split_uri
        context = self._kg["@context"] = self._kg.get("@context", dict())
        nm = self.ontology.g.namespace_manager
        space, name = split_uri(field_uri)
        if "@vocab" not in context and None in nm.namespaces():
            context["@vocab"] = nm.store.prefix(space)
        if "@vocab" in context and space == context["@vocab"]:
            # case #1, can directly use name
            return name
        if self.schema.has_field(name):
            if name not in context:
                context[name] = field_uri
            return name
        prefix = nm.store.prefix(space)
        if prefix:
            context[prefix] = space
            return nm.qname(field_uri)
        return field_uri
