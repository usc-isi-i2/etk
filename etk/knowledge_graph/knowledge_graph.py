from typing import Dict, List
import numbers
from etk.knowledge_graph_schema import KGSchema
from etk.field_types import FieldType
from etk.etk_exceptions import KgValueError, UndefinedFieldError
from etk.knowledge_graph_provenance_record import KnowledgeGraphProvenanceRecord
from etk.extraction import Extraction
from etk.segment import Segment
from etk.ontology_api import Ontology
import json


class KnowledgeGraph(Graph):
    """
    This class is a knowledge graph object, provides API for user to construct their kg.
    Add field and value to the kg object, analysis on provenance
    """

    def __init__(self, schema: KGSchema, doc) -> None:
        self.origin_doc = doc
        self.schema = schema
        if self.origin_doc.etk.generate_json_ld:
            if "doc_id" in doc.cdr_document:
                self.add_value("@id", self.origin_doc.cdr_document["doc_id"])


    def add_value(self, field_name: str, value: object = None, json_path: str = None,
                  json_path_extraction: str = None, keep_empty: bool = False) -> None:
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
        self.add_triples(URI(self.doc.doc_id), URI(field_name), URI / Literal)

    def add_triple(self, s, p, o):
        t = Triples()
        self.add_triples(t)

    def _find_types(self, triples):
        """
        find type in root level
        :param triples: 
        :return: 
        """
        types = []
        for t in triples:
            s, p, o = t
            if p == 'rdf:type':
                if isinstance(o, Triples):
                    continue
                types.append(o)
        return types

    def add_triples(self, triples, context=None):
        if not context:
            context = set([])
        s_types = self._find_types(triples)

        for t in triples:
            s, p, o = t
            o_types = []
            if isinstance(o, Triples) and o not in context:
                context.add(o)
                self.add_triples(o, context)
                o_types = self._find_types(o)

            self.schema.is_valid(s_types, p, o_types)
            self._g.add(t)

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
                prefix = [x for x in list(self.ontology.g.namespace_manager.namespaces())]
                for x, y in prefix:
                    if space[:-1] == x:
                        context[name] = str(y) + name
                        return name
                context[name] = field_uri
            return name
        prefix = nm.store.prefix(space)
        if prefix:
            context[prefix] = space
            return nm.qname(field_uri)
        return field_uri
