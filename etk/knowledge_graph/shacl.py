from pyshacl import validate
from typing import List, Dict, Union
from etk.knowledge_graph.graph import Graph
from etk.knowledge_graph.node import URI, BNode, Literal, LiteralType
from etk.knowledge_graph.subject import Subject
from rdflib import RDF, RDFS, OWL, Namespace, XSD
import rdflib


SH = Namespace('http://www.w3.org/ns/shacl#')


class SHACL(Graph):
    def __init__(self):
        super().__init__()
        self._class_nodes: Dict[str, Union[URI, BNode]] = {}

    def cnode(self, class_):
        class_ = str(class_)
        if class_ not in self._class_nodes:
            self._class_nodes[class_] = BNode()
        return self._class_nodes[class_]

    def add_ontology(self, onto_graph):
        converter = SHACLOntoConverter(self._class_nodes)
        self._g += converter.convert_ontology()


class SHACLOntoConverter:
    def __init__(self, class_nodes: Dict[str, Union[URI, BNode]] = None):
        super().__init__()
        self.onto_graph: Graph = None
        self._g = Graph()
        self._class_nodes = class_nodes if class_nodes else {}
        self._g.bind('sh', SH)

    def cnode(self, class_):
        class_ = str(class_)
        if class_ not in self._class_nodes:
            self._class_nodes[class_] = BNode()
        return self._class_nodes[class_]

    def convert_ontology(self, onto_graph: Graph) -> Graph:
        self.onto_graph = onto_graph
        # build shacl property-based NodeShape for non-domain-referenced properties
        self._convert_ontology_non_referenced_property()
        # build shacl class-based NodeShape
        # build shacl property path-based blank node for domain-referenced properties
        # for path-based property,
        # we can only add sh:datatype/sh:class based on rdfs:range
        self._convert_ontology_class_node_shape(onto_graph)

        # build shacl property based on owl restriction
        # we can add cardinality to it
        for s, p in self._g.query("""
          SELECT ?s ?p ?all ?some ?has ?exact ?min ?max
          WHERE {
            ?s rdfs:subClassOf|owl:equivalentClass ?res .
            ?res a owl:Restriction ;
                 owl:onProperty ?p .
            OPTIONAL {?res owl:allValuesFrom ?all}
            OPTIONAL {?res owl:someValuesFrom ?some}
            OPTIONAL {?res owl:hasValue ?has}
            OPTIONAL {?res owl:cardinality ?exact}
            OPTIONAL {?res owl:minCardinality ?min}
            OPTIONAL {?res owl:maxCardinality ?max}
          }
        """):
            # owl:allValuesFrom, owl:someValuesFrom, owl:hasValue
            # owl:cardinality, owl:minCardinality, owl:maxCardinality
            pass

    def _property_shape(self, property_: Union[rdflib.URIRef, str, URI]):
        if isinstance(property_, URI):
            property_ = property_.value
        property_ = rdflib.URIRef(property_)
        p_shape = Subject(BNode())
        p_shape.add_property(URI('sh:path'), URI(property_))
        ranges = list(self.onto_graph.objects(property_, RDFS.range))
        self._build_property_ranges(p_shape, ranges)
        return p_shape

    def _build_property_ranges(self, p_shape, ranges):
        if len(ranges) == 1:
            range_ = ranges[0]
            type_ = URI('sh:datatype') if self._is_datatype(range_) else URI('sh:class')
            p_shape.add_property(type_, URI(ranges[0]))
        else:
            or_list = []
            for range_ in ranges:
                range_subject = Subject(BNode())
                type_ = URI('sh:datatype') if self._is_datatype(range_) else URI('sh:class')
                range_subject.add_property(type_, URI(range_))
                or_list.append(range_subject)
            p_shape.add_property(URI('sh:or'), self._add_list(or_list))

    @staticmethod
    def _is_datatype(uri: rdflib.URIRef):
        if isinstance(uri, rdflib.BNode):
            return False
        return uri.startswith(str(XSD)) or uri.startswith(str(RDF))

    def _convert_ontology_non_referenced_property(self):
        for p, in self.onto_graph.query("""
          SELECT ?p
          WHERE {
            { ?p a rdfs:Property }
            UNION
            { ?p a owl:ObjectProperty }
            UNION
            { ?p a owl:DatatypeProperty }
            FILTER NOT EXISTS { ?p rdfs:domain ?c }
            FILTER NOT EXISTS { ?c owl:onProperty ?p }
          }
        """):
            p_shape = self._property_shape(p)
            p_shape.add_property(URI('rdf:type'), URI('sh:PropertyShape'))
            self._g.add_subject(p_shape)

    def _convert_ontology_class_node_shape(self, onto_graph: rdflib.Graph):
        # TODO
        for s in onto_graph.query("""
          SELECT ?s WHERE {
            {?s a owl:Class}
            UNION
            {?s a rdfs:Class}
            UNION
            {?p rdfs:domain ?s}
            UNION
            {?s rdfs:subClassOf|owl:equivalentClass ?o}
            UNION
            {?os rdfs:subClassOf|owl:equivalentClass ?s FILTER NOT EXISTS {?s a owl:Restriction}}
            FILTER isURI(?s)
          }
        """):
            """
            [ a sh:NodeShape ;
              sh:targetClass ?s ; 
              sh:property [ sh:path p ; sh:class class ;] ;]
            """
            node_subject = Subject(BNode())
            node_subject.add_property('rdf:type', 'sh:NodeShape')
            node_subject.add_property('sh:targetClass', URI(s))
            for range_ in onto_graph.subjects(RDFS.domain, s):
                property_subject = Subject(BNode())
                property_subject.add_property(URI('sh:path'), URI(range_))
                property_subject.add_property(URI('sh:class'))  # TODO
                node_subject.add_property(URI('sh:property'), property_subject)

    def _add_list(self, list_):
        """
        (:a :b :c) ===
        [ rdf:first :a ; rdf:rest [ rdf:first :b ; rdf:rest [ rdf:first :c ; rdf:rest rdf:nil]]]
        """
        if not list_: return URI('rdf:nil')
        head = Subject(BNode())
        head.add_property(URI('rdf:first'), list_[0])
        head.add_property(URI('rdf:rest'), self._add_list(list_[1:]))
        return head
