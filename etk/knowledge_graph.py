from typing import List, Dict
from enum import Enum, auto
from etk.document import Document
from etk.etk import ETK


class FieldType(Enum):
    """
    TEXT: value must be a string
    NUMBER: value must be a number
    """
    STRING = auto()
    NUMBER = auto()
    KG_ID = auto()


class KgSchema(object):
    """
    This class define the schema for a knowledge graph object.
    Create a knowledge graph schema according to the master config the user defined in myDIG UI
    """

    def __init__(self, config: Dict) -> None:
        self.fields_dict = {}
        try:
            for field in config["fields"]:
                if config["fields"][field]["type"] == "number":
                    self.fields_dict[field] = FieldType.NUMBER
                elif config["fields"][field]["type"] == "kg_id":
                    self.fields_dict[field] = FieldType.KG_ID
                else:
                    self.fields_dict[field] = FieldType.STRING

        except KeyError as key:
            print(str(key) + " not in config")

    @property
    def fields(self) -> List[str]:
        """
        Return a list of all fields that are defined in master config

        Args:


        Returns: List of fields
        """
        return list(self.fields_dict.keys())

    def has_field(self, field_name: str) -> bool:
        """
        Return true if the schema has the field, otherwise false

        Args:
            field_name: str

        Returns: bool
        """
        return field_name in self.fields_dict.keys()

    def field_type(self, field_name: str) -> FieldType:
        """
        Return the type of a field defined in schema, if field not defined, return None

        Args:
            field_name: str

        Returns: FieldType
        """
        if self.has_field(field_name):
            return self.fields_dict[field_name]
        else:
            print(field_name + " field not defined")

    def is_valid(self, field_name, value) -> bool:
        """
        Return true if the value type matches the defined type in schema, otherwise false.
        If field not defined, return none

        Args:
            field_name: str
            value:

        Returns: bool
        """
        if self.has_field(field_name):
            if isinstance(value, (int, float, complex)) and self.fields_dict[field_name] == FieldType.NUMBER:
                return True
            if isinstance(value, str) and self.fields_dict[field_name] == FieldType.STRING:
                return True
            return False
        else:
            print(field_name + " field not defined")


class KnowledgeGraph(object):
    """
    This class is a knowledge graph object, provides API for user to construct their kg.
    Add field and value to the kg object, analysis on provenence
    """

    def __init__(self, schema: KgSchema, doc: Document, etk: ETK) -> None:
        self._kg = {}
        self.origin_doc = doc
        self.etk = etk
        self.schema = schema

    def add_value(self, field_name: str, jsonpath: str) -> None:
        """
        Add a value to knowledge graph by giving a jsonpath

        Args:
            field_name: str
            jsonpath: str

        Returns:
        """
        if self.schema.has_field(field_name):
            self._kg[field_name] = []
            path = self.etk.invoke_parser(jsonpath)
            matches = path.find(self.origin_doc)
            for a_match in matches:
                if self.schema.is_valid(field_name, a_match.value):
                    self._kg[field_name].append({
                        "value": a_match.value,
                        "key": self.create_key_from_value(a_match.value)
                    })

    def get_kg(self) -> Dict:
        """
        Get knowledge graph object

        Args:

        Returns: Dict
        """
        return self._kg

    @staticmethod
    def create_key_from_value(value):
        """

        Args:
            value:

        Returns: key

        """
        "TODO: create function that create key from value"
        return value
