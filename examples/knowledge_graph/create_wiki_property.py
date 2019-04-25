from etk.etk import ETK
from etk.knowledge_graph import KGSchema, URI, Literal, LiteralType, Subject, Reification
from etk.extractors.glossary_extractor import GlossaryExtractor
from etk.etk_module import ETKModule
from etk.wikidata import *


class ExampleETKModule(ETKModule):
    """
    Abstract class for extraction module
    """
    def __init__(self, etk):
        ETKModule.__init__(self, etk)
        self.name_extractor = GlossaryExtractor(self.etk.load_glossary("./names.txt"), "name_extractor",
                                                self.etk.default_tokenizer, case_sensitive=False, ngrams=1)

    def process_document(self, doc):
        """
        Douglas_Adams educated_at
            value: St_John's_College
            qualifier: start_time 1971
            qualifier: end_time 1974
            reference: stated_in Encyclopædia_Britannica_Online
            rank: normal
        """
        doc.kg.bind('wikibase', 'http://wikiba.se/ontology#')
        doc.kg.bind('wd', 'http://www.wikidata.org/entity/')
        doc.kg.bind('wdt', 'http://www.wikidata.org/prop/direct/')
        doc.kg.bind('wdtn', 'http://www.wikidata.org/prop/direct-normalized/')
        doc.kg.bind('wdno', 'http://www.wikidata.org/prop/novalue/')
        doc.kg.bind('wds', 'http://www.wikidata.org/entity/statement/')
        doc.kg.bind('wdv', 'http://www.wikidata.org/value/')
        doc.kg.bind('wdref', 'http://www.wikidata.org/reference/')
        doc.kg.bind('p', 'http://www.wikidata.org/prop/')
        doc.kg.bind('pr', 'http://www.wikidata.org/prop/reference/')
        doc.kg.bind('prv', 'http://www.wikidata.org/prop/reference/value/')
        doc.kg.bind('prn', 'http://www.wikidata.org/prop/reference/value-normalized/')
        doc.kg.bind('ps', 'http://www.wikidata.org/prop/statement/')
        doc.kg.bind('psv', 'http://www.wikidata.org/prop/statement/value/')
        doc.kg.bind('psn', 'http://www.wikidata.org/prop/statement/value-normalized/')
        doc.kg.bind('pq', 'http://www.wikidata.org/prop/qualifier/')
        doc.kg.bind('pqv', 'http://www.wikidata.org/prop/qualifier/value/')
        doc.kg.bind('pqn', 'http://www.wikidata.org/prop/qualifier/value-normalized/')
        doc.kg.bind('skos', 'http://www.w3.org/2004/02/skos/core#')
        doc.kg.bind('prov', 'http://www.w3.org/ns/prov#')
        doc.kg.bind('schema', 'http://schema.org/')

        p = WDProperty('C3001', Datatype.QuantityValue)
        p.add_label('violent crime offenses', lang='en')
        p.add_description("number of violent crime offenses reported by the sheriff's office or county police department",
                          lang='en')
        p.add_statement('P31', Item('D1001'))
        p.add_statement('P1629', Item('Q1520311'))

        doc.kg.add_subject(p)
        return list()


if __name__ == "__main__":
    kg_schema = KGSchema()
    kg_schema.add_schema('@prefix : <http://isi.edu/> .', 'ttl')
    etk = ETK(kg_schema=kg_schema, modules=ExampleETKModule)
    doc = etk.create_document({}, doc_id="http://isi.edu/default-ns/projects")

    docs = etk.process_ems(doc)

    print(docs[0].kg.serialize('ttl'))
