from typing import List, Set
import numbers, re
from datetime import date, datetime
from etk.field_types import FieldType
from etk.etk_exceptions import ISODateError


class OntologyEntity(object):
    """
    Superclass of all ontology objects, including classes and properties.
    """

    def uri(self) -> str:
        """
        Returns: Tte full URI of an ontology entity.

        """
        pass

    def name(self) -> str:
        """
        The name of an ontology entity is the last part of the URI after the last / or #.

        Returns: the name part of an ontology entity.

        """
        pass

    def label(self) -> Set[str]:
        """

        Returns: the set of rdfs:label values

        """
        pass

    def definition(self) -> Set[str]:
        """

        Returns: the set of skos:definition values

        """
        pass

    def note(self) -> Set[str]:
        """

        Returns: the set of skos:note values

        """
        pass


class OntologyClass(OntologyEntity):

    def super_classes(self) -> Set[OntologyClass]:
        """
        super_classes(x) = the set of y_i such that x owl:subClassOf y_i.

        Returns: the set of super-classes.

        """
        pass

    def super_classes_closure(self) -> Set[OntologyClass]:
        """

        Returns: the transitive closure of the super_classes function

        """
        pass


class OntologyProperty(OntologyEntity):

    def super_properties(self) -> Set[OntologyProperty]:
        """
        super_properties = the set of y_i such that x owl:subPropertyOf y_i.

        Returns: the set of super_properties

        """
        pass

    def super_properties_closure(self) -> Set[OntologyProperty]:
        """

        Returns:  the transitive closure of the super_properties function

        """
        pass

    def included_domains(self) -> Set[OntologyClass]:
        """
        included_domains is the union of two sets:
        - the values of rdfs:domain
        - the values of schema:domainIncludes

        Although technically wrong, this function unions the rdfs:domain instead of intersecting them.
        A validation function prints a warning when loading the ontology.

        Returns: the set of all domains defined for a property.

        """
        pass

    def included_ranges(self) -> Set[OntologyClass]:
        """
        included_ranges is the union of two sets:
        - the values of rdfs:range
        - the values of schema:rangeIncludes

        Although technically wrong, this function unions the rdfs:range instead of intersecting them.
        A validation function prints a warning when loading the ontology.

        Returns:

        """
        pass

    def is_legal_subject(self, c: OntologyClass) -> bool:
        """
        is_legal_subject(c) = true if
        - c in included_domains(self) or
        - super_classes_closure(c) intersection included_domains(self) is not empty

        There is no need to check the included_domains(super_properties_closure(self)) because
        included_domains(super_properties_closure(self)) is subset of super_classes_closure(included_domains(self))

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
        is_legal_object(c) = true if
        - c in included_ranges(self) or
        - super_classes_closure(c) intersection included_ranges(self) is not empty

        Args:
            c:

        Returns:

        """
        pass

    def inverse(self) -> OntologyObjectProperty:
        """
        We support a compact definition as follows:
            :moved_to a owl:ObjectProperty ;
                rdfs:label "moved to" ;
                :inverse :was_destination_of ;

        When :inverse is present, our API automatically creates an OntologyObjectProperty for it, and links
        the two OntologyObjectProperty objects as inverses of each other.

        The OntologyObjectProperty that contained the :inverse definition is marked as is_primary()

        Returns: Inverse of the object property

        """
        pass

    def is_primary(self) -> bool:
        """

        Returns: True if this object property is the primary OntologyObjectProperty, see comments in inverse()

        """
        pass


class OntologyDatatypeProperty(OntologyProperty):

    def is_legal_object(self, data_type: str) -> bool:
        """
        Do data_type validation according to the rules of the XML xsd schema.

        Args:
            data_type:

        Returns:

        """
        pass


class Ontology(object):

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

    def all_properties(self) -> Set[OntologyProperty]:
        pass

    def all_classes(self) -> Set[OntologyClass]:
        pass

    def get_entity(self, uri: str) -> OntologyClass:
        """
        Find an ontology entity based on URI

        Args:
            uri:

        Returns: the OntologyEntity having the specified uri, or None

        """
        pass

    def root_classes(self) -> Set[OntologyClass]:
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
