from typing import List, Dict
import numbers, re
from datetime import date, datetime
from etk.field_types import FieldType
from etk.etk_exceptions import ISODateError


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

        self.fields_dict = dict()
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
        return field_name in self.fields_dict

    def field_type(self, field_name: str) -> FieldType:
        """
        Return the type of a field defined in schema, if field not defined, return None

        Args:
            field_name: str

        Returns: FieldType
        """
        return self.fields_dict.get(field_name, None)

        # Amandeep: The code below does not do what the description of the function says, replaced with line above
        # if self.has_field(field_name):
        #     return self.fields_dict[field_name]
        # else:
        #     print(field_name + " field not defined")

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

    @staticmethod
    def parse_number(txt_num):
        if isinstance(txt_num, str) or isinstance(txt_num, numbers.Number):

            if isinstance(txt_num, numbers.Number):
                return str(txt_num)

            try:
                # this will fail spectacularly if the data originated in europe,
                # where they use ',' inplace of '.' and vice versa for numbers

                text = txt_num.strip().replace('\n', '').replace('\t', '').replace(',', '')
                num = str(float(text)) if '.' in text else str(int(text))
                return num
            except:
                pass
        return None

    def is_valid(self, field_name, value) -> (bool, object):
        """
        Return true if the value type matches or can be coerced to the defined type in schema, otherwise false.
        If field not defined, return none

        Args:
            field_name: str
            value:

        Returns: bool, value, where the value may have been coerced to the required type.
        """

        if self.has_field(field_name):
            if self.fields_dict[field_name] == FieldType.KG_ID:
                return True, value

            if self.fields_dict[field_name] == FieldType.NUMBER:
                if isinstance(value, numbers.Number):
                    return True, value
                else:
                    converted_number = self.parse_number(value)
                    return (False, value) if not converted_number else (True, value)
            if self.fields_dict[field_name] == FieldType.STRING:
                if isinstance(value, str):
                    return True, value.strip()
                else:
                    return True, str(value).strip()

            if self.fields_dict[field_name] == FieldType.DATE:
                valid, d = self.is_date(value)
                if valid:
                    return True, d.isoformat()
                else:
                    return False, value

            if self.fields_dict[field_name] == FieldType.LOCATION:
                valid, l = self.is_location(value)
                if valid:
                    return True, l
                else:
                    return False, value
        else:
            print('{} not found in KG Schema'.format(field_name))
            return False, value
            # Amandeep: returning False instead of printing a message
            # print(field_name + " field not defined")

    @staticmethod
    def is_date(v) -> (bool, date):
        """
        Boolean function for checking if v is a date

        Args:
            v:
        Returns: bool

        """
        if isinstance(v, date):
            return True, v
        try:
            reg = r'^([0-9]{4})(?:-(0[1-9]|1[0-2])(?:-(0[1-9]|[1-2][0-9]|3[0-1])(?:T' \
                  r'([0-5][0-9])(?::([0-5][0-9])(?::([0-5][0-9]))?)?)?)?)?$'
            match = re.match(reg, v)
            if match:
                groups = match.groups()
                patterns = ['%Y', '%m', '%d', '%H', '%M', '%S']
                d = datetime.strptime('-'.join([x for x in groups if x]),
                                      '-'.join([patterns[i] for i in range(len(patterns)) if groups[i]]))
                return True, d
        except:
            pass
        return False, v

    @staticmethod
    def is_location(v) -> (bool, str):
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
            return False, v
        split_lst = v.split(":")
        if len(split_lst) != 5:
            return False, v
        if convert2float(split_lst[3]):
            longitude = abs(convert2float(split_lst[3]))
            if longitude > 90:
                return False, v
        if convert2float(split_lst[4]):
            latitude = abs(convert2float(split_lst[3]))
            if latitude > 180:
                return False, v
        return True, v
