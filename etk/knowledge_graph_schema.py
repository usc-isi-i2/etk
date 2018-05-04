from typing import List, Dict
from datetime import date, datetime
from etk.field_types import FieldType
import numbers, re


class KGSchema(object):
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
                if config["fields"][field]["type"] == "kg_id":
                    self.fields_dict[field] = FieldType.KG_ID
                elif config["fields"][field]["type"] == "number":
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
            if self.fields_dict[field_name] == FieldType.KG_ID:
                return True
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
            reg = r'^([0-9]{4})(?:-(0[1-9]|1[0-2])(?:-(0[1-9]|[1-2][0-9]|3[0-1])(?:T' \
                  r'([0-5][0-9])(?::([0-5][0-9])(?::([0-5][0-9]))?)?)?)?)?$'
            match = re.match(reg, v)
            if match:
                groups = match.groups()
                patterns = ['%Y', '%m', '%d', '%H', '%M', '%S']
                datetime.strptime('-'.join([x for x in groups if x]),
                                  '-'.join([patterns[i] for i in range(len(patterns)) if groups[i]]))
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
