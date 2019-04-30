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
        doc.kg.bind('wds', 'http://www.wikidata.org/entity/statement/')
        doc.kg.bind('wdv', 'http://www.wikidata.org/value/')
        doc.kg.bind('wdref', 'http://www.wikidata.org/reference/')
        doc.kg.bind('p', 'http://www.wikidata.org/prop/')
        doc.kg.bind('pr', 'http://www.wikidata.org/prop/reference/')
        doc.kg.bind('ps', 'http://www.wikidata.org/prop/statement/')
        doc.kg.bind('psv', 'http://www.wikidata.org/prop/statement/value/')
        doc.kg.bind('psn', 'http://www.wikidata.org/prop/statement/value-normalized/')
        doc.kg.bind('pq', 'http://www.wikidata.org/prop/qualifier/')
        doc.kg.bind('pqv', 'http://www.wikidata.org/prop/qualifier/value/')
        doc.kg.bind('skos', 'http://www.w3.org/2004/02/skos/core#')
        doc.kg.bind('prov', 'http://www.w3.org/ns/prov#')
        doc.kg.bind('schema', 'http://schema.org/')

        douglas = WDItem('Q42')
        douglas.add_label('Douglas Adams', lang='en')
        douglas.add_alias('Douglas Noël Adams', lang='fr')
        # educated at
        statement = douglas.add_statement('P69', Item('Q691283'), rank=Rank.Normal)
        # education: start time
        statement.add_qualifier('P580', TimeValue('1971',
                                                  calendar=Item('Q1985727'),
                                                  precision=Precision.year,
                                                  time_zone=0))
        # education: end time
        statement.add_qualifier('P582', TimeValue('1974',
                                                  calendar=Item('Q1985727'),
                                                  precision=Precision.year,
                                                  time_zone=0))
        # birth date
        douglas.add_statement('P569', TimeValue('1952-03-11T00:00:00+00:00',
                                                calendar=Item('Q1985727'),
                                                precision=Precision.day,
                                                time_zone=0))
        # reference
        ref_1 = WDReference()
        ref_1.add_value('P248', Item('Q5375741'))
        statement.add_reference(ref_1)

        # height
        douglas.add_statement('P2048', QuantityValue(1.96, unit=Item('Q11573')))

        # official website
        statement = douglas.add_statement('P856', URLValue('http://douglasadams.com/'))
        statement.add_qualifier('P407', Item('Q1860'))

        # Freebase ID
        douglas.add_statement('P646', ExternalIdentifier('/m/0282x', URLValue('http://g.co/kg/m/0282x')))

        doc.kg.add_subject(douglas)
        return list()


if __name__ == "__main__":
    kg_schema = KGSchema()
    kg_schema.add_schema('@prefix : <http://isi.edu/> .', 'ttl')
    etk = ETK(kg_schema=kg_schema, modules=ExampleETKModule)
    doc = etk.create_document({}, doc_id="http://isi.edu/default-ns/projects")

    docs = etk.process_ems(doc)

    print(docs[0].kg.serialize('ttl'))
