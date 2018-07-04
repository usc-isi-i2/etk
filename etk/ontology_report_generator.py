from functools import reduce
from rdflib import Graph, URIRef
from rdflib.namespace import RDFS, OWL, SKOS
from etk.ontology_api import Ontology, OntologyClass, OntologyProperty, OntologyObjectProperty
from etk.ontology_api import OntologyDatatypeProperty
from etk.ontology_namespacemanager import DIG, SCHEMA


class OntologyReportGenerator:
    def __init__(self, ontology: Ontology):
        self.ontology = ontology
        self.classes = ontology.classes
        self.properties = ontology.all_properties()
        self.data_properties = ontology.data_properties
        self.object_properties = ontology.object_properties
        self.item = '<h4 id="{}">{}</h4>\n<div class="entity">\n<table>\n{}\n</table>\n</div>'
        self.row = '<tr>\n<th align="right" valign="top">{}</th>\n<td>{}</td>\n</tr>'

    @staticmethod
    def sorted_name(arr):
        """
        Sorted input ontology entites by their names.

        :param arr: a set or list container of ontology entities
        :return: a sorted list
        """
        return sorted(arr, key=lambda x: x.name())

    def generate_html_report(self, include_turtle=False, exclude_warning=False) -> str:
        """
        Shows links to all classes and properties, a nice hierarchy of the classes, and then a nice
        description of all the classes with all the properties that apply to it.
        Example: http://www.cidoc-crm.org/sites/default/files/Documents/cidoc_crm_version_5.0.4.html

        :param include_turtle: include turtle related to this entity.
        :param exclude_warning: Exclude warning messages in HTML report
        :return: HTML in raw string
        """
        import os
        template = os.path.dirname(os.path.abspath(__file__)) + '/../ontologies/template.html'
        with open(template) as f:
            # Lists
            content = f.read().replace('{{{title}}}', 'Ontology Entities')
            content = content.replace('{{{class_list}}}', self.__html_entities_hierarchy(self.classes))
            content = content.replace('{{{dataproperty_list}}}', self.__html_entities_hierarchy(self.data_properties))
            content = content.replace('{{{objectproperty_list}}}', self.__html_entities_hierarchy(self.object_properties))
            # Classes
            content = content.replace('{{{classes}}}', self.__html_classes(include_turtle))
            # Properties
            properties = self.__html_properties(include_turtle)
            content = content.replace('{{{dataproperties}}}', properties[0])
            content = content.replace('{{{objectproperties}}}', properties[1])

            logs = '' if exclude_warning else self.ontology.log_stream.getvalue()
            content = content.replace('{{{logging}}}', '<pre><code>{}</code></pre>'.format(logs))
        return content

    def __html_classes(self, include_turtle):
        if not self.classes: return ''
        sorted_classes = self.sorted_name(self.classes)
        classes = []
        properties_map, referenced_map = self.__html_build_properties()
        for c in sorted_classes:
            attr = []
            subclass_of = self.sorted_name(c.super_classes())
            superclass_of = list(filter(lambda x: c in x.super_classes(), sorted_classes))
            if subclass_of:
                attr.append(self.row.format('Subclass of', '<br />\n'.join(map(self.__html_entity_href, subclass_of))))
            if superclass_of:
                attr.append(
                    self.row.format('Superclass of', '<br />\n'.join(map(self.__html_entity_href, superclass_of))))
            attr.extend(self.__html_entity_basic_info(c, self.row))
            properties = self.sorted_name(properties_map[c])
            if properties:
                attr.append(self.row.format('Properties', '<br />\n'.join(map(self.__html_class_property, properties))))
            properties = self.sorted_name(referenced_map[c])
            if properties:
                attr.append(self.row.format('Referenced by', '<br />\n'.join(map(self.__html_class_referenced,
                                                                            properties))))
            if include_turtle:
                code = self.__html_extract_other_info(c.uri())
                if code:
                    attr.append('<pre><code>{}</code></pre>'.format(code))
            classes.append(self.item.format('C-' + c.uri(), c.name(), '\n'.join(attr)))
        return '\n'.join(classes)
        
    def __html_properties(self, include_turtle):
        sorted_properties = self.sorted_name(self.properties)
        dataproperties, objectproperties = [], []
        for p in sorted_properties:
            attr = []
            domains = p.included_domains()
            ranges = p.included_ranges()
            if domains:
                attr.append(self.row.format('Domain', '<br />\n'.join(
                    map(self.__html_entity_href, self.sorted_name(domains)))))
            if ranges:
                if isinstance(p, OntologyObjectProperty):
                    attr.append(self.row.format('Range', '<br />\n'.join(
                        map(self.__html_entity_href, self.sorted_name(ranges)))))
                else:
                    attr.append(self.row.format('Range', '<br />\n'.join(sorted(ranges))))
            subproperty_of = self.sorted_name(p.super_properties())
            superproperty_of = list(filter(lambda x: p in x.super_properties(), sorted_properties))
            if subproperty_of:
                attr.append(self.row.format('Subproperty of', '<br />\n'.join(
                    map(self.__html_entity_href, subproperty_of))))
            if superproperty_of:
                attr.append(self.row.format('Superproperty of', '<br />\n'.join(
                    map(self.__html_entity_href, superproperty_of))))
            attr.extend(self.__html_entity_basic_info(p, self.row))
            if include_turtle:
                code = self.__html_extract_other_info(p.uri())
                if code:
                    attr.append('<pre><code>{}</code></pre>'.format(code))
            if isinstance(p, OntologyObjectProperty):
                if p.inverse():
                    attr.insert(0, self.row.format('Inverse', self.__html_entity_href(p.inverse())))
                objectproperties.append(self.item.format('O-' + p.uri(), p.name(), '\n'.join(attr)))
            else:
                dataproperties.append(self.item.format('D-' + p.uri(), p.name(), '\n'.join(attr)))
        return '\n'.join(dataproperties), '\n'.join(objectproperties)

    def __html_class_property(self, property_):
        range_ = property_.included_ranges()
        item = self.__html_entity_href(property_)
        if range_:
            tpl = ': [{}]' if len(range_) > 1 else ': {}'
            if isinstance(property_, OntologyObjectProperty):
                range_ = self.sorted_name(range_)
                item += tpl.format(', '.join(map(self.__html_entity_href, range_)))
            else:
                item += tpl.format(', '.join(range_))
        return item

    def __html_class_referenced(self, property_):
        domains = self.sorted_name(property_.included_domains())
        item = self.__html_entity_href(property_)
        if domains:
            tpl = '[{}]: ' if len(domains) > 1 else '{} :'
            item = tpl.format(', '.join(map(self.__html_entity_href, domains))) + item
        return item

    def __html_build_properties(self):
        properties_map = {c: set() for c in self.classes}
        referenced_map = {c: set() for c in self.classes}
        for property_ in self.properties:
            for domain in property_.included_domains():
                properties_map[domain].add(property_)
            if isinstance(property_, OntologyObjectProperty):
                for range_ in property_.included_ranges():
                    referenced_map[range_].add(property_)
        leaves = {c for c in self.classes} - reduce(set.union, [c.super_classes() for c in self.classes])
        while leaves:
            for class_ in leaves:
                for super_class in class_.super_classes():
                    properties_map[super_class] |= properties_map[class_]
                    referenced_map[super_class] |= referenced_map[class_]
            leaves = reduce(set.union, [c.super_classes() for c in leaves])
        return properties_map, referenced_map

    def __html_extract_other_info(self, uri):
        g = Graph()
        g.namespace_manager = self.ontology.g.namespace_manager
        if isinstance(uri, str):
            uri = URIRef(uri)
        for pred, obj in self.ontology.g.predicate_objects(uri):
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
        for obj in self.ontology.g.objects(uri, RDFS.subClassOf):
            if isinstance(self.ontology.get_entity(uri), OntologyClass) and \
                    isinstance(self.ontology.get_entity(obj), OntologyClass):
                g.remove((uri, RDFS.subClassOf, obj))
        if len(g) < 2:
            return ''
        code = g.serialize(format='turtle').decode('utf-8').split('\n')
        code = filter(bool, code)
        code = filter(lambda line: line[0]!='@', code)
        return '\n'.join(code)

    @staticmethod
    def __html_entity_basic_info(e, tpl):
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

    @staticmethod
    def __html_entity_href(e):
        template = '<a href="#{}-{}">{}</a>'
        kind = {OntologyClass: 'C', OntologyObjectProperty: 'O', OntologyDatatypeProperty: 'D'}
        return template.format(kind[type(e)], e.uri(), e.name())

