import unittest
from rdflib.namespace import XSD
from etk.ontology_api import Ontology, OntologyEntity, OntologyClass
from etk.ontology_api import OntologyProperty, OntologyDatatypeProperty, OntologyObjectProperty
from etk.ontology_api import DIG

rdf_prefix = '''@prefix : <http://isi.edu/ontologies/dig/> .
@prefix dc: <http://purl.org/dc/elements/1.1/> .
@prefix dcterms: <http://purl.org/dc/terms/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix xml: <http://www.w3.org/XML/1998/namespace> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix crm: <http://www.cidoc-crm.org/cidoc-crm/> .
@prefix schema: <http://schema.org/> .
'''

class TestOntologyAPI(unittest.TestCase):
    def test_basic_entity_info(self):
        rdf_content = rdf_prefix + '''
:Entity a owl:Class ;
    rdfs:label "Entity" ;
    skos:definition """Everything that can be described, similar to owl:Thing.""" ;
    :common_properties :label, :title, :description ;
    .
        '''
        ontology = Ontology(rdf_content)
        self.assertEqual(len(ontology.all_classes()), 1)
        for entity in ontology.all_classes():
            self.assertIsInstance(entity, OntologyEntity)
            self.assertIsInstance(entity, OntologyClass)
        self.assertEqual(len(ontology.all_properties()), 0)
        for entity in ontology.all_properties():
            self.assertIsInstance(entity, OntologyEntity)
            self.assertIsInstance(entity, OntologyProperty)
        self.assertEqual(len(ontology.root_classes()), 1)
        for entity in ontology.root_classes():
            self.assertIsInstance(entity, OntologyClass)
            self.assertEqual(len(entity.super_classes()), 0)

    def test_property_domain(self):
        rdf_content = rdf_prefix + '''
:Entity a owl:Class
    .
:Type a owl:Class
    .
:Other a owl:Class ;
    :common_properties :code
    .
:code a owl:DatatypeProperty ;
    rdfs:domain :Type ;
    schema:domainIncludes :Entity
    .
        '''
        ontology = Ontology(rdf_content)
        property_code = ontology.get_entity(DIG.code.toPython())
        self.assertIsInstance(property_code, OntologyDatatypeProperty)
        self.assertEqual(len(property_code.included_domains()), 3)
        self.assertSetEqual(property_code.included_domains(),
                            set(map(str, (DIG.Entity, DIG.Type, DIG.Other))))

    def test_datatype_property_range(self):
        rdf_content = rdf_prefix + '''
:code a owl:DatatypeProperty ;
    schema:rangeIncludes xsd:string ;
    .
:iso_code a owl:DatatypeProperty ;
    rdfs:subPropertyOf :code ;
    .
        '''
        ontology = Ontology(rdf_content)
        property_code = ontology.get_entity(DIG.code.toPython())
        property_iso_code = ontology.get_entity(DIG.code.toPython())
        self.assertIsInstance(property_iso_code, OntologyDatatypeProperty)
        self.assertTrue(property_code.is_legal_object(XSD.string.toPython()))
        self.assertTrue(property_iso_code.is_legal_object(XSD.string.toPython()))
        self.assertFalse(property_code.is_legal_object(XSD.dateTime.toPython()))
        self.assertFalse(property_iso_code.is_legal_object(XSD.dateTime.toPython()))

    def test_inverse_object_property(self):
        rdf_content = rdf_prefix + '''
:moved_to a owl:ObjectProperty ;
    rdfs:label "moved to" ;
    :inverse :was_destination_of .
        '''
        ontology = Ontology(rdf_content)
        self.assertEqual(len(ontology.all_properties()), 2)
        property_moved = ontology.get_entity(DIG.moved_to.toPython())
        property_inverse = ontology.get_entity(DIG.was_destination_of.toPython())
        self.assertIsNotNone(property_moved)
        self.assertIsNotNone(property_inverse)
        self.assertTrue(property_moved.is_primary())
        self.assertFalse(property_inverse.is_primary())
        self.assertEqual(property_moved.inverse(), property_inverse)
        self.assertEqual(property_inverse.inverse(), property_moved)

    def test_class_hierachy(self):
        rdf_content = rdf_prefix + '''
        '''
        # TODO

    def test_property_hierachy(self):
        rdf_content = rdf_prefix + '''
        '''
        # TODO

    def test_cycle_detection(self):
        rdf_content = rdf_prefix + '''
        '''
        # TODO
