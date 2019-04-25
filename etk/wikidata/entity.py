from etk.knowledge_graph.subject import Subject
from etk.knowledge_graph.node import URI, Literal
from etk.wikidata.statement import Statement, Rank
from etk.wikidata.value import Item, Property



class Entity(Subject):
    def __init__(self, node):
        super().__init__(URI('wd:'+node))
        self.node_id = node

    def add_label(self, s: str, lang='en'):
        literal = Literal(s, lang=lang)
        self.add_property(URI('rdfs:label'), literal)
        self.add_property(URI('schema:name'), literal)
        self.add_property(URI('skos:prefLabel'), literal)

    def add_alias(self, s: str, lang='en'):
        self.add_property(URI('skos:altLabel'), Literal(s, lang=lang))

    def add_description(self, s: str, lang='en'):
        self.add_property(URI('schema:description'), Literal(s, lang=lang))

    def add_statement(self, p: str, v, rank=Rank.Normal):
        statement = Statement(self.node_id, rank)
        statement.add_value(p, v)
        self.add_property(URI('p:'+p), statement)
        return statement


class WDItem(Entity, Item):
    def __init__(self, s: str):
        Entity.__init__(self, s)
        Item.__init__(self, s)
        self.add_property(URI('rdf:type'), URI('wikibase:Item'))


class WDProperty(Entity, Property):
    def __init__(self, s: str, property_type):
        Entity.__init__(s)
        Property.__init__(s)
        self.add_property(URI('rdf:type'), URI('wikibase:Property'))
        self.add_property(URI('wikibase:propertyType'), property_type) # TODO: need to change this

        self.add_property(URI('wikibase:directClaim'), URI('wdt:'+s))
        self.add_property(URI('wikibase:directClaimNormalized'), URI('wdtn:'+s))
        self.add_property(URI('wikibase:claim'), URI('p:'+s))
        self.add_property(URI('wikibase:statementProperty'), URI('ps:'+s))
        self.add_property(URI('wikibase:statementValue'), URI('psv:'+s))
        self.add_property(URI('wikibase:statementValueNormalized'), URI('psn:'+s))
        self.add_property(URI('wikibase:qualifier'), URI('pq:'+s))
        self.add_property(URI('wikibase:qualifierValue'), URI('pqv:'+s))
        self.add_property(URI('wikibase:qualifierValueNormalized'), URI('pqn:'+s))
        self.add_property(URI('wikibase:reference'), URI('pr:'+s))
        self.add_property(URI('wikibase:referenceValue'), URI('prv:'+s))
        self.add_property(URI('wikibase:referenceValueNormalized'), URI('prn:'+s))
        self.add_property(URI('wikibase:novalue'), URI('wdno:'+s))



if __name__ == '__main__':
    from etk.wikidata import *
    douglas = WDItem('Q42')
    douglas.add_label('Douglas Adams', lang='en')
    douglas.add_alias('Douglas Noël Adams', lang='fr')
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
    # height
    douglas.add_statement('P2048', QuantityValue(1.96, unit=Item('Q11573')))

    # official website
    statement = douglas.add_statement('P856', URLValue('http://douglasadams.com/'))
    statement.add_qualifier('P407', Item('Q1860'))

    # Freebase ID
    douglas.add_statement('P646', ExternalIdentifier('/m/0282x', 'http://g.co/kg/m/0282x'))

    ref_1 = WDReference()
    ref_1.add_value('P248', Item('Q5375741'))
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
    g.bind('psv', 'http://www.wikidata.org/prop/statement/value/')
    g.bind('psn', 'http://www.wikidata.org/prop/statement/value-normalized/')
    g.bind('pq', 'http://www.wikidata.org/prop/qualifier/')
    g.bind('pqv', 'http://www.wikidata.org/prop/qualifier/value/')
    g.bind('skos', 'http://www.w3.org/2004/02/skos/core#')
    g.bind('prov', 'http://www.w3.org/ns/prov#')
    g.bind('schema', 'http://schema.org/')
    g.add_subject(douglas)
    print(g.serialize('ttl'))
