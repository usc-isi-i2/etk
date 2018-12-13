import unittest
import pyshacl
import rdflib
from etk.knowledge_graph.shacl import SH, SHACL, SHACLOntoConverter
from etk.knowledge_graph.ontology import Ontology
from etk.knowledge_graph.node import URI, BNode, Literal, LiteralType
from etk.knowledge_graph.subject import Subject
from etk.knowledge_graph.graph import Graph


def subject_to_graph(subject):
    g = Graph()
    g.bind('sh', SH)
    g.bind(None, 'http://example.org/')
    g.add_subject(subject)
    return g


namespaces = {
    '': 'http://example.org/',
    'sh': SH,
    'xsd': rdflib.XSD
}


class TestPySHACL(unittest.TestCase):
    def setUp(self):
        content = """
        @prefix : <http://isi.edu/xij-rule-set#> .
        :etk a :Software ;
            :developer [ a :Developer ;
                    :name "Sylvia", "Qingyuandi" ],
                [ a :Developer ;
                    :name "Amandeep" ],
                [ a :Developer ;
                    :name "Dongyu" ],
                [ a :Developer ;
                    :name "Runqi" ] .

        :rltk a :Software ;
            :developer [ a :Developer ;
                    :name "Pedro" ],
                [ a :Developer ;
                    :name "Yixiang" ],
                [ a :Developer ;
                    :name "Mayank" ] .
        """
        self.data_graph = rdflib.Graph()
        self.data_graph.parse(data=content, format='ttl')

        ontology = """
            @prefix : <http://isi.edu/xij-rule-set#> .
            @prefix owl: <http://www.w3.org/2002/07/owl#> .
            @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
            :Person a owl:Class .
            :Developer a owl:Class ;
                       rdfs:subClassOf :Person .
            :name a owl:DatatypeProperty ;
                  rdfs:domain :Person ;
                  rdfs:range xsd:string .
        """
        self.onto_graph = rdflib.Graph()
        self.onto_graph.parse(data=ontology, format='ttl')

        shacl_content = """
        @prefix : <http://example.org/> .
        @prefix xij: <http://isi.edu/xij-rule-set#> .
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> . 
        :SoftwareShape a sh:NodeShape ;
                       sh:targetClass xij:Software ;
                       sh:property [ 
                         sh:path xij:developer ;
                         sh:minCount 1 ;
                         sh:class xij:Developer ;
                       ] .
        :PersonShape a sh:NodeShape ;
                        sh:targetClass xij:Person ;
                        sh:property [
                          sh:path xij:name ;
                          sh:minCount 1 ;
                          sh:maxCount 1 ;
                          sh:datatype xsd:string ;
                        ] .
        """
        self.shacl_graph = rdflib.Graph()
        self.shacl_graph.parse(data=shacl_content, format='ttl')

    def test_pyshacl(self):
        conforms, results_graph, results_text = pyshacl.validate(self.data_graph, shacl_graph=self.shacl_graph,
                                                                 inference='rdfs', abort_on_error=False,
                                                                 meta_shacl=False, debug=False)
        self.assertTrue(conforms)

    def test_pyshacl_with_ontology(self):
        conforms, results_graph, results_text = pyshacl.validate(self.data_graph + self.onto_graph,
                                                                 shacl_graph=self.shacl_graph, inference='rdfs',
                                                                 abort_on_error=False, meta_shacl=False, debug=False)
        self.assertFalse(conforms)


class TestSHACLOntoConverter(unittest.TestCase):
    def test_cnode(self):
        converter = SHACLOntoConverter()
        c1 = rdflib.URIRef('http://example.org/Person')
        c2 = rdflib.URIRef('http://example.org/Developer')
        cnode1 = converter.cnode(c1)
        cnode2 = converter.cnode('http://example.org/Person')
        cnode3 = converter.cnode(c2)
        self.assertIsInstance(cnode1, BNode)
        self.assertIsInstance(cnode2, BNode)
        self.assertIsInstance(cnode3, BNode)
        self.assertEqual(cnode1, cnode2)
        self.assertNotEqual(cnode1, cnode3)

        converter = SHACLOntoConverter({str(c1): URI(':Person')})
        cnode4 = converter.cnode(c1)
        self.assertEqual(cnode4, URI(':Person'))

    def test_add_list(self):
        converter = SHACLOntoConverter()
        list_subject = Subject(BNode())
        list_subject.add_property(URI(':list'), converter._add_list([URI(':a'), URI(':b'), URI(':c')]))
        list_graph = subject_to_graph(list_subject)
        self.assertTrue(list_graph._g.query("""
            PREFIX : <http://example.org/> 
            ASK { 
                ?s :list ( ?a ?b ?c ) ;
            }
        """))

    def test_property_shape(self):
        ontology = """
            @prefix : <http://example.org/> .
            @prefix owl: <http://www.w3.org/2002/07/owl#> .
            @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
            :name a owl:DatatypeProperty ;
              rdfs:range xsd:string ; .
            :developer a owl:ObjectProperty ;
              rdfs:range :PythonDeveloper, :JavaDeveloper ; .
        """
        onto_graph = Ontology()
        onto_graph.parse(ontology)
        converter = SHACLOntoConverter()
        converter.onto_graph = onto_graph._g
        p_shape = converter._property_shape('http://example.org/name')
        self.assertIsInstance(p_shape, Subject)
        p_shape_graph = subject_to_graph(p_shape)
        self.assertTrue(p_shape_graph._g.query("""
            ASK { ?s sh:path :name ;
                     sh:datatype xsd:string .
            }
        """, initNs=namespaces))

        p_shape = converter._property_shape('http://example.org/developer')
        self.assertIsInstance(p_shape, Subject)
        p_shape_graph = subject_to_graph(p_shape)
        self.assertTrue(p_shape_graph._g.query("""
            ASK { 
                ?s sh:path :developer ;
                   sh:or ( [ sh:class ?c1 ] [ sh:class ?c2 ]) .
                FILTER (?c1 != ?c2)
            }
        """, initNs=namespaces))

    def test_convert_non_referenced_property(self):
        ontology = """
            @prefix : <http://example.org/> .
            @prefix owl: <http://www.w3.org/2002/07/owl#> .
            @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
            :name a owl:DatatypeProperty ;
              rdfs:domain :Person, :Software ;
              rdfs:range xsd:string ; .
            :developer a owl:ObjectProperty ;
              rdfs:range :PythonDeveloper, :JavaDeveloper ; .
        """
        onto_graph = Ontology()
        onto_graph.parse(ontology)
        converter = SHACLOntoConverter()
        converter.onto_graph = onto_graph._g
        converter._convert_ontology_non_referenced_property()
        self.assertTrue(converter._g._g.query("""
            ASK { ?s a sh:PropertyShape ;
                     sh:path :developer ;
                     sh:or ?range .
            }
        """, initNs=namespaces))
        self.assertFalse(converter._g._g.query("ASK { ?s sh:path :name }", initNs=namespaces))

    def test_convert_ontology_class_node_shape(self):
        ontology = """
            @prefix : <http://example.org/> .
            @prefix owl: <http://www.w3.org/2002/07/owl#> .
            @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
            :Developer rdfs:subClassOf :Person .
            :name a owl:DatatypeProperty ;
              rdfs:domain :Person, :Software ;
              rdfs:range xsd:string ; .
            :developer a owl:ObjectProperty ;
              rdfs:domain :Software ;
              rdfs:range :PythonDeveloper, :JavaDeveloper ; .
        """
        onto_graph = Ontology()
        onto_graph.parse(ontology)
        converter = SHACLOntoConverter()
        converter.onto_graph = onto_graph._g
        converter._convert_ontology_class_node_shape()
        self.assertTrue(converter._g._g.query("""
            ASK { ?s a sh:NodeShape ; sh:targetClass :Developer }
        """, initNs=namespaces))
        self.assertTrue(converter._g._g.query("""
            ASK { ?s sh:targetClass :Person ; sh:property ?p }
        """, initNs=namespaces))
        self.assertTrue(converter._g._g.query("""
            ASK { 
              ?s sh:targetClass :Software ; 
                 sh:property ?p1 ;
                 sh:property ?p2 
              FILTER (?p1 != ?p2)
            }
        """, initNs=namespaces))

    def test_convert_ontology_owl_restriction(self):
        ontology = """
            @prefix : <http://example.org/> .
            @prefix owl: <http://www.w3.org/2002/07/owl#> .
            @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
            :Developer rdfs:subClassOf [ a owl:Restriction ;
                owl:onProperty :name ;
                owl:allValuesFrom xsd:string ;
                owl:cardinality 1 
            ] ; .
            :Software rdfs:subClassOf [ a owl:Restriction ;
                    owl:onProperty :name ;
                    owl:allValuesFrom xsd:string ;
                    owl:cardinality 1
                ];
                rdfs:subClassOf [ a owl:Restriction ;
                    owl:onProperty :developer ;
                    owl:allValuesFrom :Developer ;
                    owl:minCardinality 1
                ]; .
        """
        onto_graph = Ontology()
        onto_graph.parse(ontology)
        converter = SHACLOntoConverter()
        converter.onto_graph = onto_graph._g
        converter._convert_ontology_owl_restriction()
        self.assertTrue(converter._g._g.query("""
            ASK { 
                ?s a sh:NodeShape ; 
                   sh:targetClass :Developer ; 
                   sh:property ?p .
                ?p sh:path :name ;
                   sh:count 1 ;
                   sh:datatype xsd:string .
            }
        """, initNs=namespaces))
        self.assertTrue(converter._g._g.query("""
            ASK { 
                ?s sh:targetClass :Software ; 
                   sh:property ?p .
                ?p sh:path :developer ;
                   sh:minCount 1 ;
                   sh:class :Developer .
            }
        """, initNs=namespaces))

    def test_convert_ontology(self):
        ontology = """
            @prefix : <http://example.org/> .
            @prefix owl: <http://www.w3.org/2002/07/owl#> .
            @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
            :Developer rdfs:subClassOf [ a owl:Restriction ;
                owl:onProperty :name ;
                owl:allValuesFrom xsd:string ;
                owl:cardinality 1 
            ] ; .
            :Software rdfs:subClassOf [ a owl:Restriction ;
                    owl:onProperty :name ;
                    owl:allValuesFrom xsd:string ;
                    owl:cardinality 1
                ];
                rdfs:subClassOf [ a owl:Restriction ;
                    owl:onProperty :developer ;
                    owl:allValuesFrom :Developer ;
                    owl:minCardinality 1
                ]; .
        """
        onto_graph = Ontology()
        onto_graph.parse(ontology)
        converter = SHACLOntoConverter()
        shacl_graph = converter.convert_ontology(onto_graph)
        self.assertIsInstance(shacl_graph, Graph)


class TestSHACL(unittest.TestCase):
    def test_graph_validation(self):
        pass
