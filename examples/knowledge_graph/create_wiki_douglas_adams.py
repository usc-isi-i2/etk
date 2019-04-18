from etk.etk import ETK
from etk.knowledge_graph import KGSchema, URI, BNode, Literal, LiteralType, Subject, Reification
from etk.extractors.glossary_extractor import GlossaryExtractor
from etk.etk_module import ETKModule


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
        doc.kg.bind('wds', 'http://www.wikidata.org/entity/statement/')
        doc.kg.bind('wdv', 'http://www.wikidata.org/value/')
        doc.kg.bind('wdref', 'http://www.wikidata.org/reference/')
        doc.kg.bind('p', 'http://www.wikidata.org/prop/')
        doc.kg.bind('pr', 'http://www.wikidata.org/prop/reference/')
        doc.kg.bind('ps', 'http://www.wikidata.org/prop/statement/')
        doc.kg.bind('pq', 'http://www.wikidata.org/prop/qualifier/')
        doc.kg.bind('pqv', 'http://www.wikidata.org/prop/qualifier/value/')
        doc.kg.bind('skos', 'http://www.w3.org/2004/02/skos/core#')
        doc.kg.bind('prov', 'http://www.w3.org/ns/prov#')

        triple = Subject(URI('wd:Q42'))
        triple.add_property(URI('rdfs:label'), Literal('Douglas Adams', lang='en'))
        triple.add_property(URI('skos:altLabel'), Literal('Douglas Noël Adams', lang='fr'))

        reification = Reification(URI('p:P69'), URI('ps:P69'), URI('wds:q42-0E9C4724-C954-4698-84A7-5CE0D296A6F2'))
        statement = triple.add_property(URI('wdt:P69'), URI('wd:Q691283'), reification)

        # Qualifiers
        reification_start_time = Reification(URI('pqv:P580'), URI('wikibase:timeValue'),
                                             URI('wdv:cf5d396bdc1074d11438fc47ab8bcf40'))
        statement_start_time = statement.add_property(URI('pq:P580'), Literal('1 January 1971', type_=LiteralType.date), reification_start_time)
        statement_start_time.add_property(URI('rdf:type'), URI('wikibase:TimeValue'))
        statement_start_time.add_property(URI('wikibase:timeCalendarModel'), URI('wd:Q1985727'))
        statement_start_time.add_property(URI('wikibase:timePrecision'), Literal('9', type_=LiteralType.integer))
        statement_start_time.add_property(URI('wikibase:timeTimezone'), Literal('0', type_=LiteralType.integer))
        statement.add_property(URI('pq:P582'), Literal('1 January 1974', type_=LiteralType.date))

        # References
        refnode = Subject(URI('wdref:355b56329b78db22be549dec34f2570ca61ca056'))
        refnode.add_property(URI('pr:P248'), URI('wd:Q5375741'))
        statement.add_property(URI('prov:wasDerivedFrom'), refnode)

        # Ranks
        statement.add_property(URI('rdf:type'), URI('wikibase:BestRank'))
        statement.add_property(URI('wikibase:rank'), URI('wikibase:NormalRank'))

        doc.kg.add_subject(triple)
        return list()


if __name__ == "__main__":
    kg_schema = KGSchema()
    kg_schema.add_schema('@prefix : <http://isi.edu/> .', 'ttl')
    etk = ETK(kg_schema=kg_schema, modules=ExampleETKModule)
    doc = etk.create_document({}, doc_id="http://isi.edu/default-ns/projects")

    docs = etk.process_ems(doc)

    print(docs[0].kg.serialize('ttl'))
