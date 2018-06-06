from typing import List, Dict
import numbers, re
from datetime import date, datetime
from etk.field_types import FieldType
from etk.etk_exceptions import ISODateError


class OntologyEntity(object):

    def uri(self) -> str:
        pass

    def name(self) -> str:
        pass

    def label(self) -> List[str]:
        pass

    def definition(self) -> List[str]:
        pass

    def note(self) -> List[str]:
        pass


class OntologyClass(OntologyEntity):

    def super_classes(self) -> List[OntologyClass]:
        pass

    def all_super_classes(self) -> List[OntologyClass]:
        pass

    def is_super_class_of(self, c: OntologyClass) -> bool:
        """
        Args:
            c:

        Returns: true if c is this class, or this class is in all_super_classes(c).

        """
        pass


class OntologyProperty(OntologyEntity):

    def super_properties(self) -> List[OntologyProperty]:
        pass

    def all_super_properties(self) -> List[OntologyProperty]:
        pass

    def included_domains(self) -> List[OntologyClass]:
        pass

    def all_included_domains(self) -> List[OntologyClass]:
        """
        Union of the included_domains(p) for all p in all_super_properties(self)

        Returns:

        """

    def included_ranges(self) -> List[OntologyClass]:
        pass

    def is_legal_subject(self, c: OntologyClass) -> bool:
        """
        c is a legal subject if all_super_classes(c) intersect all_included_domains(self) is not empty.

        domain(friend) = Person  # Persons can have friends
        Woman subclass_of Person
        Person subclass_of Organism

        friend.is_legal_subject(Woman) -> True  # Woman can have friends
        friend.is_legal_subject(Organism) -> False  # Organism cannot have friends

        Args:
            c:

        Returns:

        """
        pass


class OntologyObjectProperty(OntologyProperty):
    def __init__(self, inverse_property=None):
        pass

    def is_legal_object(self, c: OntologyClass) -> bool:
        """
        c is a legal_object of self if c is a subclass of any class in all_included_ranges(self)
        or all_super_classes(c) intersect all_included_ranges(self) is not empty.

        Args:
            c:

        Returns:

        """
        pass

    def inverse(self) -> OntologyObjectProperty:
        """

        Returns: Inverse of the object property

        """
        pass

    def is_primary(self) -> bool:
        """

        Returns: True if this object property is the primary(compared to inverse object property)

        """
        return True


class OntologyDatatypeProperty(OntologyProperty):

    def is_legal_object(self, data_type: str) -> bool:
        pass


class KGSchema(object):

    def __init__(self, turtle: str) -> None:
        """
        Read the ontology from a string containing RDF in turtle format.

        Should do ontology validation:
        - if p.sub_property_of(q) then
          - for every x in included_domains(p) we have x is subclass of some d in included_domains(q)
          - for every y in included_ranges(p) we have y is subclass of some r in included_ranges(q) -- for object properties
        - DataType properties cannot be sub-property of Object properties, and vice versa.
        - Detect cycles in sub-class and sub-property hierarchies.

        Args:
            turtle:
        """

    def all_properties(self) -> List[OntologyProperty]:
        pass

    def all_classes(self) -> List[OntologyClass]:
        pass

    def get_property(self, uri: str) -> OntologyProperty:
        pass

    def get_class(self, uri: str) -> OntologyClass:
        pass

    def root_classes(self) -> List[OntologyClass]:
        """

        Returns: All classes that don't have a super class.

        """

    def html_documentation(self) -> str:
        """
        Example: http://www.cidoc-crm.org/sites/default/files/Documents/cidoc_crm_version_5.0.4.html
        Shows links to all classes and properties, a nice hierarchy of the classes, and then a nice
        description of all the classes with all the properties that apply to it.
        Returns:

        """
        pass
