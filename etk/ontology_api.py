import logging
from typing import Set, Union
from functools import reduce
from rdflib import Graph, URIRef
from rdflib.namespace import RDF, RDFS, OWL, SKOS, Namespace, XSD

SCHEMA = Namespace('http://schema.org/')

class OntologyEntity(object):
    """
    Superclass of all ontology objects, including classes and properties.
    """
    def __init__(self, uri):
        self._uri = uri
        self._label = set()
        self._definition = set()
        self._note = set()
        self._comment = set()
        # extract name according to the definition of name
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

    def comment(self) -> Set[str]:
        return self._comment

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
        if not self._super_classes:
            return set()
        return self._super_classes | reduce(set.union, [x._super_classes for x in self._super_classes])


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

    def super_properties_closure(self) -> Union[Set['OntologyProperty'], Set[str]]:
        """

        Returns:  the transitive closure of the super_properties function

        """
        if not self._super_properties:
            return set()
        return self._super_properties | reduce(set.union, [x.super_properties_closure() for x in self._super_properties])

    def included_domains(self) -> Set[OntologyClass]:
        """
        included_domains is the union of two sets:
        - the values of rdfs:domain
        - the values of schema:domainIncludes

        Although technically wrong, this function unions the rdfs:domain instead of intersecting them.
        A validation function prints a warning when loading the ontology.

        Returns: the set of all domains defined for a property.
        """
        return self.domains

    def included_ranges(self) -> Set[OntologyClass]:
        """
        included_ranges is the union of two sets:
        - the values of rdfs:range
        - the values of schema:rangeIncludes

        Although technically wrong, this function unions the rdfs:range instead of intersecting them.
        A validation function prints a warning when loading the ontology.

        Returns: the set of all ranges defined for a property.

        """
        return self.ranges

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
        domains = self.included_domains()
        return c in domains or c.super_classes_closure() & domains


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
        ranges = self.included_ranges()
        return c in ranges or c.super_classes_closure() & ranges

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
        return data_type in self.included_ranges() or self.super_properties() and any(x.is_legal_object(data_type) for x in self.super_properties())


class ValidationError(Exception):
    """ Base class for all validation errors """
    pass


class Ontology(object):

    def __init__(self, turtle, validation=True) -> None:
        """
        Read the ontology from a string containing RDF in turtle format.

        Should do ontology validation:
        - if p.sub_property_of(q) then
          - for every x in included_domains(p) we have x is subclass of some d in
        included_domains(q)
          - for every y in included_ranges(p) we have y is subclass of some r in
        included_ranges(q) -- for object properties
        - DataType properties cannot be sub-property of Object properties, and vice versa.
        - Detect cycles in sub-class and sub-property hierarchies.

        Args:
            turtle: str or Iterable[str]
        """
        self.entities = dict()
        self.classes = set()
        self.object_properties = set()
        self.data_properties = set()
        self.roots = set()

        g = Graph()
        if isinstance(turtle, str):
            g.parse(data=turtle, format='ttl')
        else:
            for t in turtle:
                g.parse(data=t, format='ttl')

        for ns in ('rdfs', 'rdf', 'owl', 'schema'):
            if ns not in g.namespaces():
                g.namespace_manager.bind(ns, eval(ns.upper()))

        # Class
        for uri in g.subjects(RDF.type, OWL.Class):
            if not isinstance(uri, URIRef): continue
            uri = uri.toPython()
            entity = OntologyClass(uri)
            self.entities[uri] = entity
            self.classes.add(entity)

        # Data P
        for uri in g.subjects(RDF.type, OWL.DatatypeProperty):
            uri = uri.toPython()
            entity = OntologyDatatypeProperty(uri)
            self.entities[uri] = entity
            self.data_properties.add(entity)

        # Obj P
        for uri, inv in g.query("""SELECT ?uri ?inv
                                   WHERE { ?uri a owl:ObjectProperty .
                                           OPTIONAL {?uri :inverse ?inv }}"""):
            uri = uri.toPython()
            if inv:
                inv = inv.toPython()
            entity = OntologyObjectProperty(uri, inv)
            self.entities[uri] = entity
            self.object_properties.add(entity)
            if inv:
                self.entities[inv] = entity.inverse()
                self.object_properties.add(entity.inverse())

        for entity in self.classes:
            for sub, in g.query("""SELECT ?sub
                                   WHERE  {{ ?uri rdfs:subClassOf ?sub }
                                           UNION { ?uri owl:subClassOf ?sub }}""",
                                initBindings={'uri': URIRef(entity.uri())}):
                try:
                    sub_entity = self.entities[sub.toPython()]
                    # Validation 3: Cycle detection
                    if entity in sub_entity.super_classes_closure():
                        raise ValidationError("Cycle detected")
                    entity._super_classes.add(sub_entity)
                except KeyError:
                    logging.warning("Missing a super class of :%s with uri %s.", entity.name(), sub)
            if not entity._super_classes:
                self.roots.add(entity)

        for p in self.object_properties | self.data_properties:
            for sub, in g.query("""SELECT ?sub
                                   WHERE  {{ ?uri rdfs:subPropertyOf ?sub }
                                           UNION { ?uri owl:subPropertyOf ?sub }}""",
                                initBindings={'uri': URIRef(p.uri())}):
                try:
                    sub_property = self.entities[sub.toPython()]
                    # Validation 3: Cycle detection
                    if p in sub_property.super_properties_closure():
                        raise ValidationError("Cycle detected")
                    # Validation 2: Class consistency
                    if isinstance(p, OntologyDatatypeProperty) != \
                       isinstance(sub_property, OntologyDatatypeProperty):
                        raise ValidationError("Sub-property {} should share the same class with {}"
                                              .format(sub_property.name(), p.name()))
                    p._super_properties.add(sub_property)
                except KeyError:
                    logging.warning("Missing a super property of :%s with uri %s.", p.name(), sub)

            for d, in g.query("""SELECT ?domain
                                 WHERE {{ ?uri rdfs:domain ?domain }
                                        UNION { ?uri schema:domainIncludes ?domain }
                                        UNION { ?domain :common_properties ?uri}}""",
                             initBindings={'uri': URIRef(p.uri())}):
                try:
                    domain = self.entities[d.toPython()]
                    p.domains.add(domain)
                except KeyError:
                    logging.warning("Missing a domain class of :%s with uri %s.", p.name(), d)
            for r, in g.query("""SELECT ?range
                                 WHERE {{ ?uri rdfs:range ?range }
                                        UNION { ?uri schema:rangeIncludes ?range }}""",
                             initBindings={'uri': URIRef(p.uri())}):
                if isinstance(p, OntologyDatatypeProperty):
                    p.ranges.add(r.toPython())
                else:
                    try:
                        p.ranges.add(self.entities[r.toPython()])
                    except KeyError:
                        logging.warning("Missing a domain class of :%s with uri %s.", p.name(), r)

        for entity in self.entities.values():
            uri = URIRef(entity.uri())
            for label in g.objects(uri, RDFS.label):
                entity._label.add(label.toPython())
            for definition in g.objects(uri, SKOS.definition):
                entity._definition.add(definition.toPython())
            for note in g.objects(uri, OWL.note):
                entity._note.add(note.toPython())
            for comment in g.objects(uri, RDFS.comment):
                entity._comment.add(comment.toPython())

        # After all hierarchies are built, perform the last validation
        # Validation 1: Inherited domain & range consistency
        if validation:
            for p in self.object_properties:
                self.__validation_property_domain(p)
                self.__validation_property_range(p)
            for p in self.data_properties:
                self.__validation_property_domain(p)

    def __validation_property_domain(self, p):
        for x in p.included_domains():
            for q in p.super_properties_closure():
                if x in q.included_domains():
                    logging.warning("Redundant domain :%s for :%s.", x.name(), p.name())
                for d in q.included_domains():
                    if x in d.super_classes():
                        raise ValidationError("Domain :{} of :{} is a superclass of its subproperty "
                                              " :{}'s domain {}.".format(x.name(), p.name(),
                                                                         q.name(), d.name()))

    def __validation_property_range(self, p):
        for y in p.included_ranges():
            for q in p.super_properties():
                if not any(r in y.super_classes() for r in q.included_ranges()):
                    raise ValidationError("Range {} of property {} isn't a subclass of any range of"
                                          " superproperty {}" .format(y.name(), p.name(), q.name()))

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
        return self.entities.get(str(uri), None)

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
        import os
        sorted_by_name = lambda arr: sorted(arr, key=lambda x: x.name())
        template = os.path.dirname(os.path.abspath(__file__)) + '/../ontologies/template.html'
        with open(template) as f:
            sorted_classes = sorted_by_name(self.classes)
            sorted_properties = sorted_by_name(self.all_properties())
            ### Lists
            content = f.read().replace('{{{title}}}', 'Ontology Entities') \
                              .replace('{{{class_list}}}',
                                       self.__html_entities_hierarchy(self.classes)) \
                              .replace('{{{dataproperty_list}}}',
                                       self.__html_entities_hierarchy(self.data_properties)) \
                              .replace('{{{objectproperty_list}}}',
                                       self.__html_entities_hierarchy(self.object_properties))

            ### Classes
            item = '<h4 id="{}">{}</h4>\n<div class="entity">\n<table>\n{}\n</table>\n</div>'
            row = '<tr>\n<th align="right" valign="top">{}</th>\n<td>{}</td>\n</tr>'
            classes = []
            for c in sorted_classes:
                attr = []
                subclass_of = sorted_by_name(c.super_classes())
                superclass_of = list(filter(lambda x: c in x.super_classes(), sorted_classes))
                if subclass_of:
                    attr.append(row.format('Subclass of', '<br />\n'.join(map(self.__html_entity_href, subclass_of))))
                if superclass_of:
                    attr.append(row.format('Superclass of', '<br />\n'.join(map(self.__html_entity_href, superclass_of))))
                attr.extend(self.__html_entity_basic_info(c, row))
                properties = list(filter(lambda x: c.super_classes_closure() & x.included_domains(), sorted_properties))
                if properties:
                    attr.append(row.format('Properties', '<br />\n'.join(map(self.__html_entity_href, properties))))
                properties = list(filter(lambda x: c.super_classes_closure() & x.included_ranges(), sorted_properties))
                if properties:
                    attr.append(row.format('Referenced by', '<br />\n'.join(map(self.__html_entity_href, properties))))
                classes.append(item.format('C-'+c.uri(), c.name(), '\n'.join(attr)))

            ### Properties
            dataproperties = []
            objectproperties = []
            for p in sorted_properties:
                attr = []
                domains = p.included_domains()
                ranges = p.included_ranges()
                if domains:
                    attr.append(row.format('Domain', '<br />\n'.join(map(self.__html_entity_href,
                                                                         sorted_by_name(domains)))))
                if ranges:
                    if isinstance(p, OntologyObjectProperty):
                        attr.append(row.format('Range', '<br />\n'.join(map(self.__html_entity_href,
                                                                            sorted_by_name(ranges)))))
                    else:
                        attr.append(row.format('Range', '<br />\n'.join(sorted(ranges))))
                subproperty_of = sorted_by_name(p.super_properties())
                superproperty_of = list(filter(lambda x: p in x.super_properties(), sorted_properties))
                if subproperty_of:
                    attr.append(row.format('Subproperty of', '<br />\n'
                                           .join(map(self.__html_entity_href, subproperty_of))))
                if superproperty_of:
                    attr.append(row.format('Superproperty of', '<br />\n'
                                           .join(map(self.__html_entity_href, superproperty_of))))
                attr.extend(self.__html_entity_basic_info(p, row))
                if isinstance(p, OntologyObjectProperty):
                    if p.inverse():
                        attr.insert(0, row.format('Inverse', self.__html_entity_href(p.inverse())))
                    objectproperties.append(item.format('O-'+p.uri(), p.name(), '\n'.join(attr)))
                else:
                    dataproperties.append(item.format('D-'+p.uri(), p.name(), '\n'.join(attr)))

            content = content.replace('{{{classes}}}', '\n'.join(classes)) \
                             .replace('{{{dataproperties}}}', '\n'.join(dataproperties)) \
                             .replace('{{{objectproperties}}}', '\n'.join(objectproperties))
        return content

    def __html_entity_basic_info(self, e, tpl):
        attr = list()
        basic = ['label', 'definition', 'note', 'comment']
        for info in basic:
            attr.extend([tpl.format(info, item) for item in getattr(e, info)()])
        return attr

    def __html_entities_hierarchy(self, entities):
        tree = {node: list() for node in entities}
        roots = []
        super = 'super_properties' if entities and \
            isinstance(next(iter(entities)), OntologyProperty) else 'super_classes'
        for child in entities:
            parents = getattr(child, super)()
            if not parents:
                roots.append(child)
            for parent in parents:
                tree[parent].append(child)
        def hierarchy_builder(children):
            if not children: return ''
            s = '<ul>\n'
            for child in sorted(children, key=lambda x:x.name()):
                s += '<li>{}</li>\n'.format(self.__html_entity_href(child))
                s += hierarchy_builder(tree[child])
            s += '</ul>\n'
            return s
        return hierarchy_builder(roots)

    def __html_entity_href(self, e):
        template = '<a href="#{}-{}">{}</a>'
        kind = {OntologyClass: 'C', OntologyObjectProperty: 'O', OntologyDatatypeProperty: 'D'}
        return template.format(kind[type(e)], e.uri(), e.name())

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Generate HTML report for the input ontology files')
    parser.add_argument('files', nargs='+', help='Input turtle files.')
    parser.add_argument('--no-validation', action='store_false', dest='validation', default=True,
                        help='Don\'t perform domain and range validation.')
    parser.add_argument('-o', '--output', dest='out', default='ontology-doc.html',
                        help='Location of generated HTML report.')
    args = parser.parse_args()

    contents = [open(f).read() for f in args.files]
    ontology = Ontology(contents, validation=args.validation)
    doc_content = ontology.html_documentation()

    with open(args.out, "w") as f:
        f.write(doc_content)

