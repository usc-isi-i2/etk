from typing import List, Dict
from enum import Enum, auto
from etk.document import Document
from etk.etk import ETK
from etk.exception import KgValueInvalidError, ISODateError, InvalidJsonPathError
from datetime import date, datetime
import numbers


class FieldType(Enum):
    """
    TEXT: value must be a string
    NUMBER: value must be a number
    LOCATION: value must be a location format
    DATE: value must be a date format
    """
    STRING = auto()
    NUMBER = auto()
    LOCATION = auto()
    DATE = auto()


class KgSchema(object):
    """
    This class define the schema for a knowledge graph object.
    Create a knowledge graph schema according to the master config the user defined in myDIG UI
    """

    def __init__(self, config: Dict) -> None:
        """
        Record a mapping about each fields and its type from config file

        Args:
            config: Dict
        """

        self.fields_dict = {}
        try:
            for field in config["fields"]:
                if config["fields"][field]["type"] == "number":
                    self.fields_dict[field] = FieldType.NUMBER
                elif config["fields"][field]["type"] == "date":
                    self.fields_dict[field] = FieldType.DATE
                elif config["fields"][field]["type"] == "location":
                    self.fields_dict[field] = FieldType.LOCATION
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
            if isinstance(value, numbers.Number) and self.fields_dict[field_name] == FieldType.NUMBER:
                return True
            if isinstance(value, str) and self.fields_dict[field_name] == FieldType.STRING:
                return True
            if self.is_date(value) and self.fields_dict[field_name] == FieldType.DATE:
                return True
            if self.is_location(value) and self.fields_dict[field_name] == FieldType.LOCATION:
                return True
            return False
        else:
            print(field_name + " field not defined")

    @staticmethod
    def is_date(v):
        """
        Boolean function for checking if v is a date

        Args:
            v:
        Returns: bool

        """

        if isinstance(v, date):
            return True
        try:
            datetime.strptime(v, '%Y-%m-%d')
            return True
        except:
            try:
                datetime.strptime(v, '%Y-%m-%dT%H:%M:%S')
                return True
            except:
                pass
        return False

    @staticmethod
    def is_location(v):
        """
        Boolean function for checking if v is a location format

        Args:
            v:
        Returns: bool

        """

        def convert2float(value):
            try:
                float_num = float(value)
                return float_num
            except ValueError:
                return False

        if not isinstance(v, str):
            return False
        split_lst = v.split(":")
        if len(split_lst) != 5:
            return False
        if convert2float(split_lst[3]):
            longitude = abs(convert2float(split_lst[3]))
            if longitude > 90:
                return False
        if convert2float(split_lst[4]):
            latitude = abs(convert2float(split_lst[3]))
            if latitude > 180:
                return False
        return True


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
                path = self.etk.invoke_parser(jsonpath)
                try:
                    matches = path.find(self.origin_doc.value)
                except Exception:
                    raise InvalidJsonPathError("Invalid Json Path")

                all_valid = True
                for a_match in matches:
                    if self.schema.is_valid(field_name, a_match.value):
                        this_value = self.value_pre_process(a_match.value, field_name)
                        self._kg[field_name].append({
                            "value": this_value,
                            "key": self.create_key_from_value(this_value, field_name)
                        })
                    else:
                        all_valid = False
                if not all_valid:
                    raise KgValueInvalidError("Some Type of Kg Value Invalid")

            else:
                print("===Field already in kg, skip the adding===")

    def add_value(self, field_name: str, value) -> None:
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
                self._kg[field_name].append({
                    "value": value,
                    "key": self.create_key_from_value(value, field_name)
                })
            elif isinstance(value, list):
                all_valid = True
                for a_value in value:
                    if self.schema.is_valid(field_name, a_value):
                        a_value = self.value_pre_process(a_value, field_name)
                        self._kg[field_name].append({
                            "value": a_value,
                            "key": self.create_key_from_value(a_value, field_name)
                        })
                    else:
                        all_valid = False
                if not all_valid:
                    raise KgValueInvalidError("Some Type of Kg Value Invalid")
            else:
                raise KgValueInvalidError("Invalid type of kg value: " + type(value))

        else:
            print("===Field already in kg, skip the adding===")

    def get_kg(self) -> Dict:
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
                    return d+"T00:00:00"
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
