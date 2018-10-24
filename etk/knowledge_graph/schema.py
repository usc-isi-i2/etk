from typing import List, Dict, Union
import numbers, re
from datetime import date, datetime
from etk.field_types import FieldType
from etk.etk_exceptions import ISODateError
from etk.knowledge_graph.ontology import Ontology
from etk.knowledge_graph.triples import Triples
from etk.knowledge_graph.node import URI, BNode, Literal, LiteralType
from etk.utilities import deprecated
import json


class KGSchema(object):
    """
    This class define the schema for a knowledge graph object.
    Create a knowledge graph schema according to the master config the user defined in myDIG UI
    """
    def __init__(self, content=None) -> None:
        self.ontology = Ontology()
        if content:
            self.add_schema(content, 'master_config')

    def add_schema(self, content: str, format: str) -> None:
        """
        Add schema file into the ontology.
        :param content: schema content
        :param format: schema format, can be in 'master_config' or any RDF format
        """
        if format == 'master_config':
            if isinstance(content, dict):
                config = content
            else:
                config = json.loads(content)
            self._add_master_config(config)
        else:
            self.ontology.parse(content, format)

    def _add_master_config(self, config):
        self.ontology._ns.bind_for_master_config()
        try:
            for field in config["fields"]:
                t = Triples(URI(field))
                if config["fields"][field]["type"] == "kg_id":
                    t.add_property(URI('rdf:type'), URI('owl:ObjectProperty'))
                elif config["fields"][field]["type"] == "number":
                    t.add_property(URI('rdf:type'), URI('owl:DatatypeProperty'))
                    # t.add_property(URI('rdf:range'), URI('xsd:int'))
                elif config["fields"][field]["type"] == "date":
                    t.add_property(URI('rdf:type'), URI('owl:DatatypeProperty'))
                elif config["fields"][field]["type"] == "location":
                    t.add_property(URI('rdf:type'), URI('owl:DatatypeProperty'))
                    t.add_property(URI('rdf:range'), URI('xsd:string'))
                else:
                    t.add_property(URI('rdf:type'), URI('owl:DatatypeProperty'))
                    t.add_property(URI('rdf:range'), URI('xsd:string'))
                if "description" in config["fields"][field] and config["fields"][field]["description"]:
                    t.add_property(URI('rdf:comment'), Literal(config["fields"][field]["description"]))
                self.ontology.add_triples(t)
        except KeyError as key:
            print(str(key) + " not in config")

    def is_valid(self, s_types: List[URI], p: URI, o_types: List[URI]) -> bool:
        """
        Check if it's a valid triple by checking the property's domain and range
        :param s_types: the types of the subject
        :param p: the property
        :param o_types: the types of the object
        :return: bool
        """
        return self.ontology.is_valid(s_types, p, o_types)

    @property
    def fields(self) -> List[str]:
        """
        Return a list of all fields that are defined in master config
        """
        return list({"{}:{}".format(prefix, property_) for prefix, property_ in map(
            self.ontology._ns.split_uri,
            self.ontology.object_properties | self.ontology.datatype_properties
        )})

    @deprecated()
    def has_field(self, field_name: str) -> bool:
        """
        Return true if the schema has the field, otherwise false
        """
        property_ = self.ontology._resolve_URI(URI(field_name))
        return property_ in self.ontology.object_properties or property_ in self.ontology.datatype_properties

    @deprecated()
    def field_type(self, field_name: str, value: object) -> Union[Triples, URI, Literal, None]:
        """
        Return the type of a field defined in schema, if field not defined, return None
        """
        property_ = self.ontology._resolve_URI(URI(field_name))
        if property_ in self.ontology.object_properties:
            return value if isinstance(value, Triples) else URI(value)
        elif property_ in self.ontology.datatype_properties:
            # TODO: check Literal type
            return Literal(value)
        else:
            return None

    @staticmethod
    def iso_date(d) -> str:
        """
        Return iso format of a date
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

    @staticmethod
    def is_date(v) -> (bool, date):
        """
        Boolean function for checking if v is a date
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
