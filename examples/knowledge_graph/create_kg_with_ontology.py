from etk.etk import ETK
from etk.knowledge_graph.schema import KGSchema
from etk.extractors.glossary_extractor import GlossaryExtractor
from etk.etk_module import ETKModule
from etk.knowledge_graph.node import URI, Literal
from etk.knowledge_graph.triples import Triples


class ExampleETKModule(ETKModule):
    """
    Abstract class for extraction module
    """
    def __init__(self, etk):
        ETKModule.__init__(self, etk)
        self.name_extractor = GlossaryExtractor(self.etk.load_glossary("./names.txt"), "name_extractor",
                                                self.etk.default_tokenizer,
                                                case_sensitive=False, ngrams=1)

    def process_document(self, doc):
        """
        Add your code for processing the document
        """

        descriptions = doc.select_segments("projects[*].description")
        names = doc.select_segments("projects[*].name")
        projects = doc.select_segments("projects[*]")

        doc.kg.bind(None, 'http://isi.edu/default-ns/')

        for p, n, d in zip(projects, names, descriptions):
            triple = Triples(URI(n.value))
            developers = doc.extract(self.name_extractor, d)
            p.store(developers, "members")
            for developer in developers:
                triple.add_property(URI("developer"), Literal(developer.value))
            doc.kg.add_triples(triple)

        return list()


if __name__ == "__main__":

    sample_input = {
        "projects": [
            {
                "name": "etk",
                "description": "version 2 of etk, implemented by Runqi Shao, Dongyu Li, Sylvia lin, Amandeep and "
                               "others."
            },
            {
                "name": "rltk",
                "description": "record linkage toolkit, implemented by Pedro, Mayank, Yixiang and several students."
            }
        ]
    }

    ontology = """
@prefix : <http://isi.edu/xij-rule-set#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
:Software a owl:Class ;
          rdfs:label "Software" .
:developer a owl:DatatypeProperty ;
          rdfs:label "developer" ;
          rdf:domain :Software ;
          rdf:range xsd:string .
    """

    kg_schema = KGSchema()
    kg_schema.add_schema(ontology, format='ttl')
    etk = ETK(kg_schema=kg_schema, modules=ExampleETKModule)
    doc = etk.create_document(sample_input)

    docs = etk.process_ems(doc)

    print(docs[0].kg.serialize('ttl'))
