import os, sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from etk.etk import ETK
from etk.knowledge_graph import KGSchema
from etk.etk_module import ETKModule
from etk.ontology_api import Ontology, rdf_generation
from rdflib.namespace import RDF


class ExampleETKModule(ETKModule):
    def __init__(self, etk):
        ETKModule.__init__(self, etk)

    def process_document(self, doc):
        docs = list()
        for data in doc.cdr_document.get('data', []):
            docs.extend(self.__create_document(data))
        return docs

    def __create_document(self, data):
        type_ = data.get(RDF.type.toPython(), list())
        type_.extend(data.get('@type', list()))
        doc = self.etk.create_document(dict(), doc_id=data['@id'], type_=type_)
        docs = [doc]
        for key in data:
            if key in {'@id', '@type', RDF.type.toPython()}:
                continue
            field_name = doc.kg.context_resolve(key)
            for value in data[key]:
                if isinstance(value, dict):
                    # at most nest 1 level, no need to worry what it returns
                    subdoc = self.__create_document(value)
                    docs.append(subdoc[0])
                    doc.kg.add_value(field_name, subdoc[0].doc_id)
                else:
                    doc.kg.add_value(field_name, value)
        return docs


if __name__ == "__main__":
    import json

    sample_input = '''
[
  {
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#type": [
      "http://dig.isi.edu/ontologies/dig/Organization",
      "http://dig.isi.edu/ontologies/dig/Entity"
    ],
    "http://www.w3.org/2004/02/skos/core#prefLabel": [
      "Jack Daniels"
    ],
    "@id": "http://www.isi.edu/dig/entities/eacb308a-7f69-49fa-bab9-4d25ffb09a15"
  },
  {
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#type": [
      "http://dig.isi.edu/ontologies/dig/MOVEMENT_TRANSPORT",
      "http://dig.isi.edu/ontologies/dig/Event"
    ],
    "http://www.w3.org/2004/02/skos/core#prefLabel": [
      "travelled"
    ],
    "@id": "http://www.isi.edu/dig/events/dabaf6a2-744b-4f0a-a872-3c11c4aea0a9"
  },
  {
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#type": [
      "http://dig.isi.edu/ontologies/dig/Event",
      "http://dig.isi.edu/ontologies/dig/CONFLICT_ATTACK"
    ],
    "http://www.w3.org/2004/02/skos/core#prefLabel": [
      "shot"
    ],
    "http://dig.isi.edu/ontologies/dig/conflict_attack_place": [
      {
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type": [
          "http://dig.isi.edu/ontologies/dig/Entity",
          "http://dig.isi.edu/ontologies/dig/GeopoliticalEntity"
        ],
        "http://www.w3.org/2004/02/skos/core#prefLabel": [
          "Ukrainian",
          "Ukraines",
          "Ukraine",
          "Better Ukraine.",
          "Ukraine's",
          "Ukrainians"
        ],
        "@id": "http://www.isi.edu/dig/entities/dc81cfec-0606-43d2-829f-bb1b02046172"
      },
      "http://www.isi.edu/dig/entities/dc81cfec-0606-43d2-829f-bb1b02046173"
    ],
    "@id": "http://www.isi.edu/dig/events/ee6c93c3-0b16-4494-926e-f61d7d11b101"
  }
]
    '''
    ontology_content = '''
@prefix : <http://dig.isi.edu/ontologies/dig/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix schema: <http://schema.org/> .
:Event a owl:Class ; .
:Entity a owl:Class ; .
:Organization a owl:Class ; .
:MOVEMENT_TRANSPORT a owl:Class ; .
:GeopoliticalEntity a owl:Class ; .
skos:prefLabel a owl:DatatypeProperty ; 
    schema:domainIncludes :Entity, :Event ;
    rdfs:range xsd:string ; .
:conflict_attack_place a owl:ObjectProperty ;
    schema:domainIncludes :Entity, :Event ;
    schema:rangeIncludes :GeopoliticalEntity ; .
    '''

    ontology = Ontology(ontology_content, validation=False, include_undefined_class=True, quiet=True)
    kg_schema = KGSchema(ontology.merge_with_master_config(dict()))
    etk = ETK(modules=ExampleETKModule, kg_schema=kg_schema, ontology=ontology, generate_json_ld=True)
    input_data = {'doc_id': '1', 'data': json.loads(sample_input)}
    doc = etk.create_document(input_data)
    docs = etk.process_ems(doc)
    kgs = [json.dumps(doc.kg.value) for doc in docs[1:]]
    with open('output.jsonl', 'w') as f:
        f.write('\n'.join(kgs))
    with open('output.nt', 'w') as f:
        f.writelines(map(rdf_generation, kgs))

