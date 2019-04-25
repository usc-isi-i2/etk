from etk.knowledge_graph.subject import Subject, Reification
from etk.knowledge_graph.node import URI, Literal
from etk.wikidata.statement import Statement
import uuid
from enum import Enum


class Rank(Enum):
    Normal = URI('wikibase:NormalRank')
    Preferred = URI('wikibase:PreferredRank')
    Deprecated = URI('wikibase:DeprecatedRank')


class WDItem(Subject):
    def __init__(self, s: str):
        if not isinstance(s, URI):
            s = URI('wd:'+s)
        super().__init__(s)
        self.add_property(URI('rdf:type'), URI('wikibase:Item'))

    def add_label(self, s: str, lang='en'):
        self.add_property(URI('rdfs:label'), Literal(s, lang=lang))

    def add_alias(self, s: str, lang='en'):
        self.add_property(URI('skos:altLabel'), Literal(s, lang=lang))

    def add_statement(self, p: str, o: URI, rank=Rank.Normal):
        statement = Statement(URI('p:'+p), URI('ps:'+p))
        self.add_property(URI('wdt:'+p), o, statement)

        # TODO: who will add BestRank?
        # statement_subject.add_property(URI('rdf:type'), URI('wikibase:BestRank'))
        statement.add_property(URI('wikibase:rank'), rank.value)

        return statement


class WDRef(Subject):
    def __init__(self):
        super().__init__(URI('wdref:'+str(uuid.uuid4())))
        super().add_property(URI('rdf:type'), URI('wikibase:Reference'))

    def add_property(self, p, o, reify=None):
        p = 'pr:' + p
        super().add_property(URI(p), o)


if __name__ == '__main__':
    from etk.wikidata.value import TimeValue, Precision
    douglas = WDItem('Q42')
    douglas.add_label('Douglas Adams', lang='en')
    douglas.add_alias('Douglas Noël Adams', lang='fr')
    statement = douglas.add_statement('P69', URI('wd:Q691283'), rank=Rank.Normal)
    # start time
    statement.add_qualifier('P580', TimeValue('1971',
                                              calendar=URI('wd:Q1985727'),
                                              precision=Precision.year,
                                              time_zone=0))
    # end time
    statement.add_qualifier('P582', TimeValue('1974',
                                              calendar=URI('wd:Q1985727'),
                                              precision=Precision.year,
                                              time_zone=0))
    ref_1 = WDRef()
    ref_1.add_property('P248', URI('wd:Q5375741'))
    statement.add_reference(ref_1)

    # Test with Graph
    from etk.knowledge_graph.graph import Graph
    g = Graph()
    g.bind('', 'http://wikiba.se/ontology#')
    g.bind('wikibase', 'http://wikiba.se/ontology#')
    g.bind('wd', 'http://www.wikidata.org/entity/')
    g.bind('wdt', 'http://www.wikidata.org/prop/direct/')
    g.bind('wds', 'http://www.wikidata.org/entity/statement/')
    g.bind('wdv', 'http://www.wikidata.org/value/')
    g.bind('wdref', 'http://www.wikidata.org/reference/')
    g.bind('p', 'http://www.wikidata.org/prop/')
    g.bind('pr', 'http://www.wikidata.org/prop/reference/')
    g.bind('ps', 'http://www.wikidata.org/prop/statement/')
    g.bind('pq', 'http://www.wikidata.org/prop/qualifier/')
    g.bind('pqv', 'http://www.wikidata.org/prop/qualifier/value/')
    g.bind('skos', 'http://www.w3.org/2004/02/skos/core#')
    g.bind('prov', 'http://www.w3.org/ns/prov#')
    g.add_subject(douglas)
    print(g.serialize('ttl'))
