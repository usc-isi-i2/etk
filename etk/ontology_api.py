import logging
from typing import Set, Union
from functools import reduce
from rdflib import Graph, URIRef
from rdflib.namespace import RDF, RDFS, OWL, SKOS, XSD
from etk.ontology_namespacemanager import OntologyNamespaceManager, DIG, SCHEMA



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
        return self._super_classes | reduce(set.union, [x.super_classes() for x in self._super_classes])


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
        return self._super_properties | reduce(set.union, [x.super_properties_closure()
                                                           for x in self._super_properties])

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
        return data_type in self.included_ranges() or self.super_properties() and \
               any(x.is_legal_object(data_type) for x in self.super_properties())


class ValidationError(Exception):
    """ Base class for all validation errors """
    pass


class Ontology(object):
    def __init__(self, turtle, validation=True, include_undefined_class=False, quiet=False) -> None:
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
        import io, sys

        self.entities = dict()
        self.classes = set()
        self.object_properties = set()
        self.data_properties = set()
        self.log_stream = io.StringIO()
        logging.getLogger().addHandler(logging.StreamHandler(self.log_stream))
        if not quiet:
            logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

        self.g = self.__init_graph_parse(turtle)

        # Class
        for uri in self.g.subjects(RDF.type, OWL.Class):
            self.__init_ontology_class(uri)
        # Datatype Property
        for uri in self.g.subjects(RDF.type, OWL.DatatypeProperty):
            self.__init_ontology_datatype_property(uri)
        # Object Property
        for uri, inv in self.g.query("""SELECT ?uri ?inv
                                   WHERE { ?uri a owl:ObjectProperty .
                                           OPTIONAL {?uri dig:inverse ?inv }}"""):
            self.__init_ontology_object_property(uri, inv)
        # subClassOf
        for uri, sub in self.g.query("""SELECT ?uri ?sub
                                    WHERE  {{ ?uri rdfs:subClassOf ?sub }
                                              UNION { ?uri owl:subClassOf ?sub }}"""):
            self.__init_ontology_subClassOf(uri, sub, include_undefined_class)

        # subPropertyOf
        for uri, sub in self.g.query("""SELECT ?uri ?sub
                                   WHERE  {{ ?uri rdfs:subPropertyOf ?sub }
                                           UNION { ?uri owl:subPropertyOf ?sub }}"""):
            self.__init_ontology_subPropertyOf(uri, sub)

        # domain
        for uri, d in self.g.query("""SELECT ?uri ?domain
                             WHERE {{ ?uri rdfs:domain ?domain }
                                    UNION { ?uri schema:domainIncludes ?domain }
                                    UNION { ?domain dig:common_properties ?uri}}"""):
            self.__init_ontology_property_domain(uri, d, include_undefined_class)

        for uri, r in self.g.query("""SELECT ?uri ?range
                             WHERE {{ ?uri rdfs:range ?range }
                                    UNION { ?uri schema:rangeIncludes ?range }}"""):
            self.__init_ontology_property_range(uri, r, include_undefined_class)

        for entity in self.entities.values():
            uri = URIRef(entity.uri())
            for label in self.g.objects(uri, RDFS.label):
                entity._label.add(label.toPython())
            for definition in self.g.objects(uri, SKOS.definition):
                entity._definition.add(definition.toPython())
            for note in self.g.objects(uri, OWL.note):
                entity._note.add(note.toPython())
            for comment in self.g.objects(uri, RDFS.comment):
                entity._comment.add(comment.toPython())

        # After all hierarchies are built, perform the last validation
        # Validation 1: Inherited domain & range consistency
        if validation:
            for p in self.object_properties:
                self.__validation_property_domain(p)
                self.__validation_property_range(p)
            for p in self.data_properties:
                self.__validation_property_domain(p)

    def __init_graph_parse(self, contents):
        nm = OntologyNamespaceManager(Graph())
        g = nm.graph
        if isinstance(contents, str):
            g.parse(data=contents, format='ttl')
        else:
            for content in contents:
                g.parse(data=content, format='ttl')
        return g

    def __init_ontology_class(self, uri):
        if isinstance(uri, URIRef):
            uri = uri.toPython()
        else:
            return
        entity = OntologyClass(uri)
        self.entities[uri] = entity
        self.classes.add(entity)
        return entity

    def __init_ontology_datatype_property(self, uri):
        uri = uri.toPython()
        entity = OntologyDatatypeProperty(uri)
        self.entities[uri] = entity
        self.data_properties.add(entity)
        return entity

    def __init_ontology_object_property(self, uri, inv):
        uri = uri.toPython()
        entity = OntologyObjectProperty(uri, inv)
        self.entities[uri] = entity
        self.object_properties.add(entity)
        if inv:
            inv = inv.toPython()
            self.entities[inv] = entity.inverse()
            self.object_properties.add(entity.inverse())
        return entity

    def __init_ontology_subClassOf(self, uri, sub, include_undefined_class):
        entity = self.get_entity(uri.toPython())
        if not entity:
            if include_undefined_class:
                entity = self.__init_ontology_class(uri)
            else:
                logging.warning("Missing a class with uri %s.", uri)
                return
        if not isinstance(entity, OntologyClass):
            logging.warning("Subclass %s is not a class.", uri)
            return
        sub_entity = self.get_entity(sub.toPython())
        if not sub_entity:
            if include_undefined_class:
                sub_entity = self.__init_ontology_class(sub)
            else:
                logging.warning("Missing a super class of %s with uri %s.", uri, sub)
                return
        if not isinstance(sub_entity, OntologyClass):
            logging.warning("Superclass %s is not a class.", sub)
            return
        # Validation 3: Cycle detection
        if entity in sub_entity.super_classes_closure():
            raise ValidationError("Cycle detected")
        entity._super_classes.add(sub_entity)

    def __init_ontology_subPropertyOf(self, uri, sub):
        property_ = self.get_entity(uri.toPython())
        sub_property = self.get_entity(sub.toPython())
        if not property_:
            logging.warning("Missing a property with URI: %s", uri)
            return
        if not isinstance(property_, OntologyProperty):
            logging.warning("Subproperty %s is not a property.", uri)
            return
        if not sub_property:
            logging.warning("Misssing a subproperty of %s with URI %s", uri, sub)
            return
        if not isinstance(sub_property, OntologyProperty):
            logging.warning("Superproperty %s is not a property.", sub)
        # Validation 3: Cycle detection
        if property_ in sub_property.super_properties_closure():
            raise ValidationError("Cycle detected")
        # Validation 2: Class consistency
        if isinstance(property_, OntologyDatatypeProperty) != \
                isinstance(sub_property, OntologyDatatypeProperty):
            raise ValidationError("Subproperty {} should share the same class with {}"
                                  .format(sub_property.name(), property_.name()))
        property_._super_properties.add(sub_property)

    def __init_ontology_property_domain(self, uri, d, include_undefined_class):
        property_ = self.get_entity(uri.toPython())
        domain = self.get_entity(d.toPython())
        if not (property_ and isinstance(property_, OntologyProperty)):
            return
        if not domain:
            if include_undefined_class:
                domain = self.__init_ontology_class(d)
            else:
                logging.warning("Missing a domain class for :%s with uri %s.", property_.name(), d)
                return
        property_.domains.add(domain)

    def __init_ontology_property_range(self, uri, r, include_undefined_class):
        property_ = self.get_entity(uri.toPython())
        if not (property_ and isinstance(property_, OntologyProperty)):
            return
        if isinstance(property_, OntologyDatatypeProperty):
            property_.ranges.add(r.toPython())
            return
        range_ = self.get_entity(r.toPython())
        if not range_:
            if include_undefined_class:
                range_ = self.__init_ontology_class(r)
            else:
                logging.warning("Missing a range class for :%s with uri %s.", property_.name(), r)
                return
        if not isinstance(range_, OntologyClass):
            return
        property_.ranges.add(range_)

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
                                          " superproperty {}".format(y.name(), p.name(), q.name()))

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
        return set(filter(lambda e: not e.super_classes(), self.classes))

    def merge_with_master_config(self, config, defaults={}, delete_orphan_fields=False) -> str:
        if isinstance(config, str):
            import json
            config = json.loads(config)
        properties = self.all_properties()
        config['fields'] = config.get('fields', dict())
        fields = config['fields']

        d_color = defaults.get('color', 'white')
        d_icon = defaults.get('icon', 'icons:default')

        if delete_orphan_fields:
            exist = {p.name() for p in properties}
            unexist = set(fields.keys()) - exist
            for name in unexist:
                del fields[name]

        for p in properties:
            field = fields.get(p.name(), {'show_in_search': False,
                                          'combine_fields': False,
                                          'number_of_rules': 0,
                                          'glossaries': [],
                                          'use_in_network_search': False,
                                          'case_sensitive': False,
                                          'show_as_link': 'text',
                                          'blacklists': [],
                                          'show_in_result': 'no',
                                          'rule_extractor_enabled': False,
                                          'search_importance': 1,
                                          'group_name': '',
                                          'show_in_facets': False,
                                          'predefined_extractor': 'none',
                                          'rule_extraction_target': ''})
            config['fields'][p.name()] = field
            field['screen_label'] = ' '.join(p.label())
            field['description'] = '\n'.join(p.definition())
            field['name'] = p.name()

            # color
            if 'color' not in field:
                color = self.__merge_close_ancestor_color(p, fields, attr='color')
                field['color'] = color if color else d_color
            # icon
            if 'icon' not in field:
                icon = self.__merge_close_ancestor_color(p, fields, attr='icon')
                fields['icon'] = icon if icon else d_icon
            # type
            if isinstance(p, OntologyObjectProperty):
                field['type'] = 'kg_id'
            else:
                try:
                    field['type'] = self.__merge_xsd_to_type(next(iter(p.included_ranges())))
                except StopIteration:
                    field['type'] = None
        return config

    def __merge_close_ancestor_color(self, property_, fields, attr):
        from collections import deque
        added = property_.super_properties()
        ancestors = deque(added)
        while ancestors:
            super_property = ancestors.popleft()
            name = super_property.name()
            if name in fields and attr in fields[name]:
                return fields[name][attr]
            ancestors.extend(list(super_property.super_properties() - added))

    def __merge_xsd_to_type(self, uri):
        xsd_ref = {
            XSD.string: 'string',
            XSD.boolean: 'number',
            XSD.decimal: 'number',
            XSD.float: 'number',
            XSD.double: 'number',
            XSD.duration: 'number',
            XSD.dateTime: 'date',
            XSD.time: 'date',
            XSD.date: 'date',
            XSD.gYearMonth: 'date',
            XSD.gYear: 'date',
            XSD.gMonthDay: 'date',
            XSD.gMonth: 'date',
            XSD.gDay: 'date',
            XSD.hexBinary: 'string',
            XSD.base64Binary: 'string',
            XSD.anyURI: 'string',
            XSD.QName: 'string',
            XSD.NOTATION: 'string'
        }
        return xsd_ref.get(URIRef(uri), None)

    def html_documentation(self, include_turtle=False, exclude_warning=False) -> str:
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
            properties_map, referenced_map = self.__html_build_properties()
            for c in sorted_classes:
                attr = []
                subclass_of = sorted_by_name(c.super_classes())
                superclass_of = list(filter(lambda x: c in x.super_classes(), sorted_classes))
                if subclass_of:
                    attr.append(row.format('Subclass of', '<br />\n'.join(map(self.__html_entity_href, subclass_of))))
                if superclass_of:
                    attr.append(
                        row.format('Superclass of', '<br />\n'.join(map(self.__html_entity_href, superclass_of))))
                attr.extend(self.__html_entity_basic_info(c, row))
                properties = sorted_by_name(properties_map[c])
                if properties:
                    attr.append(row.format('Properties', '<br />\n'.join(map(self.__html_class_property, properties))))
                properties = sorted_by_name(referenced_map[c])
                if properties:
                    attr.append(row.format('Referenced by', '<br />\n'.join(map(self.__html_class_referenced,
                                                                                properties))))
                if include_turtle:
                    code = self.__html_extract_other_info(c.uri())
                    if code:
                        attr.append('<pre><code>{}</code></pre>'.format(code))
                classes.append(item.format('C-'+c.uri(), c.name(), '\n'.join(attr)))

            ### Properties
            dataproperties, objectproperties = [], []
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
                        attr.append(row.format('Range', '<br />\n'.join(map(self.g.namespace_manager.qname,
                                                                            sorted(ranges)))))
                subproperty_of = sorted_by_name(p.super_properties())
                superproperty_of = list(filter(lambda x: p in x.super_properties(), sorted_properties))
                if subproperty_of:
                    attr.append(row.format('Subproperty of', '<br />\n'
                                           .join(map(self.__html_entity_href, subproperty_of))))
                if superproperty_of:
                    attr.append(row.format('Superproperty of', '<br />\n'
                                           .join(map(self.__html_entity_href, superproperty_of))))
                attr.extend(self.__html_entity_basic_info(p, row))
                if include_turtle:
                    code = self.__html_extract_other_info(p.uri())
                    if code:
                        attr.append('<pre><code>{}</code></pre>'.format(code))
                if isinstance(p, OntologyObjectProperty):
                    if p.inverse():
                        attr.insert(0, row.format('Inverse', self.__html_entity_href(p.inverse())))
                    objectproperties.append(item.format('O-'+p.uri(), p.name(), '\n'.join(attr)))
                else:
                    dataproperties.append(item.format('D-'+p.uri(), p.name(), '\n'.join(attr)))

            content = content.replace('{{{classes}}}', '\n'.join(classes)) \
                             .replace('{{{dataproperties}}}', '\n'.join(dataproperties)) \
                             .replace('{{{objectproperties}}}', '\n'.join(objectproperties))
            logs = '' if exclude_warning else self.log_stream.getvalue()
            content = content.replace('{{{logging}}}', '<pre><code>{}</code></pre>'.format(logs))
        return content

    def __html_class_property(self, property_):
        range_ = property_.included_ranges()
        item = self.__html_entity_href(property_)
        if range_:
            tpl = ': [{}]' if len(range_) > 1 else ': {}'
            if isinstance(property_, OntologyObjectProperty):
                range_ = sorted(range_, key=lambda x: x.name())
                item += tpl.format(', '.join(map(self.__html_entity_href, range_)))
            else:
                item += tpl.format(', '.join(map(self.g.namespace_manager.qname, sorted(range_))))
        return item

    def __html_class_referenced(self, property_):
        domains = sorted(property_.included_domains(), key=lambda x: x.name())
        item = self.__html_entity_href(property_)
        if domains:
            tpl = '[{}]: ' if len(domains) > 1 else '{} :'
            item = tpl.format(', '.join(map(self.__html_entity_href, domains))) + item
        return item

    def __html_build_properties(self):
        properties_map = {c: set() for c in self.all_classes()}
        referenced_map = {c: set() for c in self.all_classes()}
        for property_ in self.all_properties():
            for domain in property_.included_domains():
                properties_map[domain].add(property_)
            if isinstance(property_, OntologyObjectProperty):
                for range_ in property_.included_ranges():
                    referenced_map[range_].add(property_)
        leaves = {c for c in self.all_classes()} - reduce(set.union, [c.super_classes() for c in self.all_classes()])
        while leaves:
            for class_ in leaves:
                for super_class in class_.super_classes():
                    properties_map[super_class] |= properties_map[class_]
                    referenced_map[super_class] |= referenced_map[class_]
            leaves = reduce(set.union, [c.super_classes() for c in leaves])
        return properties_map, referenced_map

    def __html_extract_other_info(self, uri):
        g = Graph()
        g.namespace_manager = self.g.namespace_manager
        if isinstance(uri, str):
            uri = URIRef(uri)
        for pred, obj in self.g.predicate_objects(uri):
            g.add((uri, pred, obj))
        g.remove((uri, DIG.commen_properties, None))
        g.remove((uri, SCHEMA.domainIncludes, None))
        g.remove((uri, SCHEMA.rangeIncludes, None))
        g.remove((uri, RDFS.domain, None))
        g.remove((uri, RDFS.range, None))
        g.remove((uri, RDFS.label, None))
        g.remove((uri, RDFS.comment, None))
        g.remove((uri, SKOS.definition, None))
        g.remove((uri, SKOS.note, None))
        g.remove((uri, OWL.subPropertyOf, None))
        for obj in self.g.objects(uri, RDFS.subClassOf):
            if isinstance(self.get_entity(uri), OntologyClass) and \
                    isinstance(self.get_entity(obj), OntologyClass):
                g.remove((uri, RDFS.subClassOf, obj))
        if len(g) < 2:
            return ''
        code = g.serialize(format='turtle').decode('utf-8').split('\n')
        code = filter(bool, code)
        code = filter(lambda line: line[0]!='@', code)
        return '\n'.join(code)

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
            for child in sorted(children, key=lambda x: x.name()):
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
    parser.add_argument('-i', '--include-undefined-classes', action='store_true',
                        dest='include_class', default=False, help='Include those undefined classes '
                                                                  'but referenced by others.')
    parser.add_argument('-t', '--include-turtle', action='store_true', dest='include_turtle',
                        default=False, help='Include turtle related to this entity. NOTE: this may '
                                            'takes longer time.')
    parser.add_argument('-q', '--quiet', action='store_true', dest='quiet', default=False,
                        help='Suppress warning.')
    parser.add_argument('--exclude-warning', action='store_true', dest='exclude_warning',
                        default=False, help='Exclude warning messages in HTML report')
    args = parser.parse_args()

    contents = [open(f).read() for f in args.files]
    ontology = Ontology(contents, validation=args.validation, include_undefined_class=args.include_class,
                        quiet=args.quiet)
    doc_content = ontology.html_documentation(include_turtle=args.include_turtle,
                                              exclude_warning=args.exclude_warning)

    with open(args.out, "w") as f:
        f.write(doc_content)
