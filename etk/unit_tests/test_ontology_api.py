import unittest
from rdflib.namespace import XSD
from etk.ontology_api import Ontology, OntologyEntity, OntologyClass
from etk.ontology_api import OntologyProperty, OntologyDatatypeProperty, OntologyObjectProperty
from etk.ontology_api import ValidationError
from etk.ontology_namespacemanager import DIG, SCHEMA


rdf_prefix = '''@prefix : <http://dig.isi.edu/ontologies/dig/> .
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
                            set(map(ontology.get_entity, map(str, (DIG.Entity, DIG.Type, DIG.Other)))))

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
:Entity a owl:Class ; .
:Group a owl:Class ;
    rdfs:subClassOf :Entity ; .
:Actor a owl:Class ;
    owl:subClassOf :Group ; .
        '''
        ontology = Ontology(rdf_content)
        self.assertEqual(len(ontology.all_classes()), 3)
        entity = ontology.get_entity(DIG.Entity.toPython())
        self.assertIsNotNone(entity)
        self.assertSetEqual(ontology.root_classes(), {entity})
        group = ontology.get_entity(DIG.Group.toPython())
        self.assertIsNotNone(group)
        actor = ontology.get_entity(DIG.Actor.toPython())
        self.assertIsNotNone(actor)
        self.assertSetEqual(group.super_classes(), {entity})
        self.assertSetEqual(actor.super_classes(), {group})
        self.assertSetEqual(actor.super_classes_closure(), {entity, group})

    def test_property_hierachy(self):
        rdf_content = rdf_prefix + '''
:code a owl:DatatypeProperty ; .

:iso_code a owl:DatatypeProperty ;
    rdfs:subPropertyOf :code ; .

:cameo_code a owl:DatatypeProperty ;
    rdfs:subPropertyOf :code ; .

:iso_country_code a owl:DatatypeProperty ;
    rdfs:subPropertyOf :iso_code ; .

:adm1_code a owl:DatatypeProperty ;
    rdfs:subPropertyOf :iso_code ; .
        '''
        ontology = Ontology(rdf_content)
        self.assertEqual(len(ontology.all_classes()), 0)
        self.assertEqual(len(ontology.all_properties()), 5)
        code = ontology.get_entity(DIG.code.toPython())
        self.assertIsNotNone(code)
        self.assertIsInstance(code, OntologyDatatypeProperty)
        iso_code = ontology.get_entity(DIG.iso_code.toPython())
        self.assertIsNotNone(iso_code)
        iso_country_code = ontology.get_entity(DIG.iso_country_code.toPython())
        self.assertIsNotNone(iso_country_code)
        self.assertSetEqual(iso_code.super_properties(), {code})
        self.assertSetEqual(iso_country_code.super_properties_closure(), {code, iso_code})

    def test_class_cycle_detection(self):
        rdf_content = rdf_prefix + '''
:Entity a owl:Class ;
    rdfs:label "Entity" ;
    skos:definition """Everything that can be described, similar to owl:Thing.""" ;
    owl:subClassOf :Actor ;
    :common_properties :label, :title, :description ;
    :crm_equivalent crm:E1_CRM_Entity
    .

:Group a owl:Class ;
    rdfs:label "Group" ;
    skos:definition """A """ ;
    rdfs:subClassOf :Entity ;
    :crm_equivalent crm:E74_Group ;
    :common_properties :label, :title ;
    .

:Actor a owl:Class ;
    rdfs:label "Actor" ;
    skos:definition """A group, organization, person who have the potential to do actions.""" ;
    rdfs:subClassOf :Group ;
    :crm_equivalent crm:E39_Actor ;
    :common_properties :label, :title, :religion ;
    .
        '''
        with self.assertRaises(ValidationError):
            Ontology(rdf_content)

    def test_property_cycle_detection(self):
        rdf_content = rdf_prefix + '''
:Entity a owl:Class ; .
:code a owl:DatatypeProperty ;
    rdfs:label "code" ;
    skos:definition """A short string used as a code for an entity.""" ;
    skos:note """Examples are country codes, currency codes.""" ;
    schema:domainIncludes :Entity ;
    schema:rangeIncludes xsd:string ;
    rdfs:subPropertyOf :iso_code ;
    .

:iso_code a owl:DatatypeProperty ;
    rdfs:label "ISO code" ;
    skos:definition """A sanctioned ISO code for something.""" ;
    rdfs:subPropertyOf :code ;
    .
        '''
        with self.assertRaises(ValidationError):
            Ontology(rdf_content)

    def test_property_class_consistency(self):
        rdf_content = rdf_prefix + '''
:Entity a owl:Class ; .
:Place a owl:Class ; .
:code a owl:DatatypeProperty ;
    schema:domainIncludes :Entity ;
    schema:rangeIncludes xsd:string ; .

:country a owl:ObjectProperty ;
    rdfs:subPropertyOf :code ;
    schema:domainIncludes :Place ;
    schema:rangeIncludes :Country ; .
        '''
        with self.assertRaises(ValidationError):
            Ontology(rdf_content)

    def test_property_domain_inherit_consistency(self):
        rdf_content = rdf_prefix + '''
:Thing a owl:Class ; .
:Event a owl:Class ;
    rdfs:subClassOf :Thing ; .

:had_participant a owl:ObjectProperty ;
    schema:domainIncludes :Event ; .

:transferred_ownership_to a owl:ObjectProperty ;
    schema:domainIncludes :Thing ;
    owl:subPropertyOf :carried_out_by ; .

:carried_out_by a owl:ObjectProperty ;
    owl:subPropertyOf :had_participant ; .
        '''
        with self.assertRaises(ValidationError):
            Ontology(rdf_content)

    def test_property_domain_inherit_redundant(self):
        rdf_content = rdf_prefix + '''
:Event a owl:Class ; .
:had_participant a owl:ObjectProperty ;
    schema:domainIncludes :Event ;
    .
:transferred_ownership_to a owl:ObjectProperty ;
    schema:domainIncludes :Event ;
    owl:subPropertyOf :carried_out_by ;
    .
:carried_out_by a owl:ObjectProperty ;
    owl:subPropertyOf :had_participant ;
    .
        '''
        with self.assertLogs(level="WARNING"):
            Ontology(rdf_content)

    def test_property_range_inherit_consistency(self):
        rdf_content = rdf_prefix + '''
:Event a owl:Class ; .
:Entity a owl:Class ;
    .
:Group a owl:Class ;
    rdfs:subClassOf :Entity ;
    .
:Actor a owl:Class ;
    rdfs:subClassOf :Group ;
    .
:Physical_Object a owl:Class ;
    rdfs:subClassOf :Entity ;
    .
:Man_Made_Thing a owl:Class ;
    rdfs:subClassOf :Entity ;
    .
:had_participant a owl:ObjectProperty ;
    schema:rangeIncludes :Actor ;
    .
:transferred_custody_of a owl:ObjectProperty ;
    schema:rangeIncludes :Physical_Object, :Man_Made_Thing ;
    owl:subPropertyOf :had_participant ;
    .
        '''
        with self.assertRaises(ValidationError):
            Ontology(rdf_content)

    def test_property_merge_master_config(self):
        rdf_content = rdf_prefix + ':had_participant a owl:ObjectProperty . '
        ontology = Ontology(rdf_content)
        d_color = 'red'
        config = ontology.merge_with_master_config('{}', {'color': d_color})
        self.assertIn('fields', config)
        fields = config['fields']
        self.assertIn('had_participant', fields)
        property_ = fields['had_participant']
        self.assertIn('color', property_)
        self.assertEqual(property_['color'], d_color)

    def test_property_merge_config_inherit(self):
        rdf_content = rdf_prefix + '''
:had_participant a owl:ObjectProperty ; .
:transferred_custody_of a owl:ObjectProperty ;
    owl:subPropertyOf :had_participant ; .
        '''
        ontology = Ontology(rdf_content)
        config = ontology.merge_with_master_config('{"fields":{"had_participant":{"color":"red"}}}')
        self.assertIn('fields', config)
        fields = config['fields']
        self.assertIn('had_participant', fields)
        property_ = fields['had_participant']
        self.assertIn('color', property_)
        self.assertEqual(property_['color'], 'red')
        self.assertIn('transferred_custody_of', fields)
        sub_property = fields['transferred_custody_of']
        self.assertIn('color', sub_property)
        self.assertEqual(property_['color'], sub_property['color'])

    def test_property_merge_config_closest_inherit(self):
        rdf_content = rdf_prefix + '''
:had_participant a owl:ObjectProperty ; .
:carried_out_by a owl:ObjectProperty ;
    owl:subPropertyOf :had_participant ; .
:custody_received_by a owl:ObjectProperty ;
    owl:subPropertyOf :carried_out_by ; .
        '''
        ontology = Ontology(rdf_content)
        config = ontology.merge_with_master_config('{"fields":{"had_participant":{"color":"red"},'
                                                   '"carried_out_by":{"color":"blue"}}}')
        self.assertTrue('fields' in config)
        fields = config['fields']
        self.assertIn('had_participant', fields)
        property_ = fields['had_participant']
        self.assertIn('color', property_)
        self.assertEqual('red', property_['color'])
        self.assertIn('carried_out_by', fields)
        sub_property = fields['carried_out_by']
        self.assertIn('color', sub_property)
        self.assertEqual('blue', sub_property['color'])
        self.assertIn('custody_received_by', fields)
        subsub = fields['custody_received_by']
        self.assertIn('color', subsub)
        self.assertEqual(sub_property['color'], subsub['color'])

    def test_property_merge_config_delete_orphan(self):
        rdf_content = rdf_prefix + '''
:had_participant a owl:ObjectProperty ; .
:carried_out_by a owl:ObjectProperty ;
    owl:subPropertyOf :had_participant ; .
        '''
        ontology = Ontology(rdf_content)
        config = ontology.merge_with_master_config('{"fields":{"had_participant":{"color":"red"},'
                                                   '"custody_received_by":{"color":"blue"}}}',
                                                   delete_orphan_fields=True)
        self.assertTrue('fields' in config)
        fields = config['fields']
        self.assertIn('had_participant', fields)
        self.assertIn('carried_out_by', fields)
        self.assertNotIn('custody_received_by', fields)

    def test_rdf_generation(self):
        from etk.ontology_api import rdf_generation
        kg = '''
{
  "@id": "http://www.isi.edu/aida/events/dabaf6a2-744b-4f0a-a872-3c11c4aea0a9",
  "@type": ["dig:Person", "dig:Entity"],
  "label": [{
    "@value": "Jason"
  }, {
    "@value": "json"
  }],
  "@context": {
    "@vocab": "http://www.w3.org/2000/01/rdf-schema#",
    "dig": "http://dig.isi.edu/ontologies/dig/"
  }
}
        '''
        nt = rdf_generation(kg)
        self.assertIsInstance(nt, str)
        self.assertEqual(4, len([*filter(bool, nt.split('\n'))]))

    def test_ontology_api_is_valid_with_empty_kg(self):
        import json

        rdf_content = rdf_prefix + '''
:Place a owl:Class ;
    :common_properties :region ; .
:region a owl:DatatypeProperty ;
    schema:domainIncludes :Place ;
    schema:rangeIncludes xsd:string ; .
            '''
        kg = '{}'
        ontology = Ontology(rdf_content)
        self.assertTrue(ontology.is_valid('region', 'somewhere', json.loads(kg)))
        self.assertFalse(ontology.is_valid('region', 1, json.loads(kg)))
        self.assertFalse(ontology.is_valid('region', True, json.loads(kg)))

    def test_ontology_api_is_valid_with_kg(self):
        import json

        rdf_content = rdf_prefix + '''
:Human a owl:Class ; .
:Place a owl:Class ;
    :common_properties :region ; .
:region a owl:DatatypeProperty ;
    schema:domainIncludes :Place ;
    schema:rangeIncludes xsd:string ; .
            '''
        kg = '''
{
  "@type": ["dig:Place"],
  "@id": "some_doc_id",
  "@context": {
    "dig": "http://dig.isi.edu/ontologies/dig/"
  }
}
        '''
        kg_wrong_domain = '''
{
  "@type": ["dig:Human"],
  "@id": "some_doc_id",
  "@context": {
    "dig": "http://dig.isi.edu/ontologies/dig/"
  }
}
        '''
        kg_domain_doesnt_exist = '''
{
  "@type": ["dig:People"],
  "@id": "some_doc_id",
  "@context": {
    "dig": "http://dig.isi.edu/ontologies/dig/"
  }
}
        '''
        ontology = Ontology(rdf_content)
        self.assertTrue(ontology.is_valid('region', 'somewhere', json.loads(kg)))
        self.assertFalse(ontology.is_valid('region', 'somewhere', json.loads(kg_wrong_domain)))
        self.assertFalse(ontology.is_valid('region', 'somewhere', json.loads(kg_domain_doesnt_exist)))
