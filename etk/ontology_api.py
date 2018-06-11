from typing import List, Set
import numbers, re
from datetime import date, datetime
from etk.field_types import FieldType
from etk.etk_exceptions import ISODateError
from rdflib import Graph, URIRef
from rdflib.namespace import RDF, RDFS, OWL, SKOS, Namespace
from functools import reduce
import logging


class OntologyEntity(object):
    """
    Superclass of all ontology objects, including classes and properties.
    """
    def __init__(self, uri):
        self._uri = uri
        self._label = set()
        self._definition = set()
        self._note = set()
        ind = self._uri.rfind('#')
        if ind == -1:
            ind = self._uri.rfind('/')
        self._name = self._uri[ind+1:]

    def uri(self) -> str:
        """
        Returns: Tte full URI of an ontology entity.

        """
        return self._uri

    def name(self) -> str:
        """
        The name of an ontology entity is the last part of the URI after the last / or #.

        Returns: the name part of an ontology entity.

        """
        return self._name

    def label(self) -> Set[str]:
        """

        Returns: the set of rdfs:label values

        """
        return self._label

    def definition(self) -> Set[str]:
        """

        Returns: the set of skos:definition values

        """
        return self._definition

    def note(self) -> Set[str]:
        """

        Returns: the set of skos:note values

        """
        return self._note

    def __str__(self):
        s = "<{}>".format(self._uri)
        for label in self._label:
            s += '\n\trdfs:label "{}";'.format(label)
        for d in self._definition:
            s += '\n\tskos:definition "{}"'.format(d)
        for n in self._note:
            s += '\n\tskos:note "{}"'.format(n)
        return s


class OntologyClass(OntologyEntity):
    def __init__(self, uri):
        super().__init__(uri)
        self._super_classes = set()

    def super_classes(self) -> Set['OntologyClass']:
        """
        super_classes(x) = the set of y_i such that x owl:subClassOf y_i.

        Returns: the set of super-classes.

        """
        return self._super_classes

    def super_classes_closure(self) -> Set['OntologyClass']:
        """

        Returns: the transitive closure of the super_classes function

        """
        closure = set()
        level = self._super_classes
        while level and not level < closure:
            if self in closure:
                print("Validation Error, Cycle in subClassOf dependency")
            closure |= level
            level = reduce(set.union, [x._super_classes for x in level])
        return closure

class OntologyProperty(OntologyEntity):
    def __init__(self, uri):
        super().__init__(uri)
        self.domains = set()
        self.ranges = set()
        self._super_properties = set()

    def super_properties(self) -> Set['OntologyProperty']:
        """
        super_properties = the set of y_i such that x owl:subPropertyOf y_i.

        Returns: the set of super_properties

        """
        return self._super_properties

    def super_properties_closure(self) -> Set['OntologyProperty']:
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
    def __init__(self, uri, inverse_property=None):
        super().__init__(uri)
        self._inverse = None
        self._primary = True
        if inverse_property:
            self._inverse = OntologyObjectProperty(inverse_property)
            self._inverse._inverse = self
            self._inverse._primary = False

    def is_legal_object(self, c: OntologyClass) -> bool:
        """
        is_legal_object(c) = true if
        - c in included_ranges(self) or
        - super_classes_closure(c) intersection included_ranges(self) is not empty

        Args:
            c:

        Returns:

        """
        # TODO
        pass

    def inverse(self) -> 'OntologyObjectProperty':
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
        return self._inverse

    def is_primary(self) -> bool:
        """

        Returns: True if this object property is the primary OntologyObjectProperty, see comments in inverse()

        """
        return self._primary


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
        DIG = Namespace('http://isi.edu/ontologies/dig')
        SCHEMA = Namespace('http://schema.org/')
        self.entities = dict()
        self.classes = set()
        self.object_properties = set()
        self.data_properties = set()
        self.roots = set()

        g = Graph()
        g.parse(data=turtle, format='ttl')

        # Class
        for uri in g.subjects(RDF.type, OWL.Class):
            uri = uri.toPython()
            c = OntologyClass(uri)
            self.entities[uri] = c
            self.classes.add(c)

        # Data P
        for uri in g.subjects(RDF.type, OWL.DatatypeProperty):
            uri = uri.toPython()
            dp = OntologyDatatypeProperty(uri)
            self.entities[uri] = dp
            self.data_properties.add(dp)

        # Obj P
        for uri, inv in g.query("""SELECT ?uri ?inv
                                   WHERE { ?uri a owl:ObjectProperty .
                                           OPTIONAL {?uri :inverse ?inv }}"""):
            uri = uri.toPython()
            if inv:
                inv = inv.toPython()
            op = OntologyObjectProperty(uri, inv)
            self.entities[uri] = op
            self.object_properties.add(op)
            if inv:
                self.entities[inv] = op.inverse()
                self.object_properties.add(op.inverse())

        for c in self.classes:
            # for sub in g.objects(c.uri(), RDFS.subClassOf):
            for sub, in g.query("""SELECT ?sub
                                   WHERE  {{ ?uri rdfs:subClassOf ?sub }
                                           UNION { ?uri owl:subClassOf ?sub }}""",
                                initBindings={'uri': c.uri()}):
                sub = sub.toPython()
                c._super_classes.add(self.entities[sub])
            if not c._super_classes:
                self.roots.add(c)
            # for p in g.objects(c.uri(), DIG.common_properties):
            for p in g.objects(URIRef(c.uri()), DIG.common_properties):
                p = p.toPython()
                if p in self.entities:
                    self.entities[p].domains.add(c)

        for p in self.object_properties | self.data_properties:
            for sub, in g.query("""SELECT ?sub
                                   WHERE  {{ ?uri rdfs:subPropertyOf ?sub }
                                           UNION { ?uri owl:subPropertyOf ?sub }}""",
                                initBindings={'uri': p.uri()}):
                sub = sub.toPython()
                try:
                    p._super_properties.add(self.entities[sub])
                except KeyError:
                    logging.warning("Missing a super property of :{} with uri {}.".format(p.name(), sub))
            for d in g.objects(URIRef(p.uri()), SCHEMA.domainIncludes):
                p.domains.add(d.toPython())
            for r in g.objects(URIRef(p.uri()), SCHEMA.rangeIncludes):
                p.ranges.add(r.toPython())

        for entity in self.entities.values():
            uri = URIRef(entity.uri())
            for label in g.objects(uri, RDFS.label):
                entity._label.add(label)
            for definition in g.objects(uri, SKOS.definition):
                entity._definition.add(definition)
            for note in g.objects(uri, OWL.note):
                entity._note.add(note)

    def all_properties(self) -> Set[OntologyProperty]:
        return self.data_properties | self.object_properties

    def all_classes(self) -> Set[OntologyClass]:
        return self.classes

    def get_entity(self, uri: str) -> OntologyClass:
        """
        Find an ontology entity based on URI

        Args:
            uri:

        Returns: the OntologyEntity having the specified uri, or None

        """
        if uri in self.entities:
            return self.entities[uri]
        return None

    def root_classes(self) -> Set[OntologyClass]:
        """

        Returns: All classes that don't have a super class.

        """
        return self.roots

    def html_documentation(self) -> str:
        """
        Example: http://www.cidoc-crm.org/sites/default/files/Documents/cidoc_crm_version_5.0.4.html
        Shows links to all classes and properties, a nice hierarchy of the classes, and then a nice
        description of all the classes with all the properties that apply to it.
        Returns:

        """
        content = ''
        with open('../ontologies/template.html') as f:
            sorted_classes = sorted(self.classes, key=lambda x:x.name())
            content = f.read().replace('{{{title}}}', 'OntologyEntities')
            content = content.replace('{{{class_list}}}', self._html_class_hierachy()) \
                             .replace('{{{dataproperty_list}}}', self._html_dataproperty_hierachy()) \
                             .replace('{{{objectproperty_list}}}', self._html_objectproperty_hierachy())

            item = '<h4 id="{}">{}</h4><div><table>{}</table></div>'
            row = '<tr><th>{}</th><td>{}</td></tr>'
            classes = []
            for c in sorted_classes:
                attr = []
                attr.append(row.format('label', ', '.join(c.label())))
                attr.append(row.format('definition', ', '.join(c.definition())))
                attr.append(row.format('note', ''.join(c.note())))
                classes.append(item.format('C-'+c.name(), c.name(), ''.join(attr)))

            dataproperties = ''
            objectproperties = ''
            content = content.replace('{{{classes}}}', ''.join(classes)) \
                             .replace('{{{dataproperties}}}', dataproperties) \
                             .replace('{{{objectproperties}}}', objectproperties)
        with open('../ontologies/output.html', 'w') as f:
            f.write(content)
        return content

    def _html_class_hierachy(self):
        tree = {node: list() for node in self.classes}
        for child in self.classes:
            for parent in child.super_classes():
                tree[parent].append(child)
        # TODO validate cycle before recursion
        def hierachy_builder(children):
            if not children: return ''
            s = '<ul>'
            for child in sorted(children, key=lambda x:x.name()):
                s += '<li><a href="#C-{}">{}</a></li>'.format(child.name(), child.name())
                s += hierachy_builder(tree[child])
            s += '</ul>'
            return s
        return hierachy_builder(self.roots)

    def _html_dataproperty_hierachy(self):
        tree = {node: list() for node in self.data_properties}
        roots = []
        for child in self.data_properties:
            if not child.super_properties():
                roots.append(child)
            for parent in child.super_properties():
                tree[parent].append(child)
        def hierachy_builder(children):
            if not children: return ''
            s = '<ul>'
            for child in sorted(children, key=lambda x:x.name()):
                s += '<li><a href="#D-{}">{}</a></li>'.format(child.name(), child.name())
                s += hierachy_builder(tree[child])
            s += '</ul>'
            return s
        return hierachy_builder(roots)

    def _html_objectproperty_hierachy(self):
        tree = {node: list() for node in self.object_properties}
        roots = []
        for child in self.object_properties:
            if not child.super_properties():
                roots.append(child)
            for parent in child.super_properties():
                tree[parent].append(child)
        def hierachy_builder(children):
            if not children: return ''
            s = '<ul>'
            for child in sorted(children, key=lambda x:x.name()):
                s += '<li><a href="#O-{}">{}</a></li>'.format(child.name(), child.name())
                s += hierachy_builder(tree[child])
            s += '</ul>'
            return s
        return hierachy_builder(roots)
