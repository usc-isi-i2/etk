from etk.knowledge_graph.subject import Subject
from etk.knowledge_graph.node import URI, Literal
from etk.wikidata.statement import Statement, Rank
from etk.wikidata.value import Item, Property, Datatype
from collections import defaultdict
import warnings


change_recorder = set()


def serialize_change_record(fp):
    fp.writelines('{}\t{}\n'.format(node, prop) for node, prop in change_recorder)


class Entity(Subject):

    def __init__(self, node, creator, namespace='wd'):
        super().__init__(URI(namespace + ':'+node))
        self.node_id = node
        self.creator = URI(creator) if creator else None
        self.namespace = namespace

    def add_label(self, s: str, lang='en'):
        literal = Literal(s, lang=lang)
        self.add_property(URI('rdfs:label'), literal)
        self.add_property(URI('schema:name'), literal)
        self.add_property(URI('skos:prefLabel'), literal)

    def add_alias(self, s: str, lang='en'):
        self.add_property(URI('skos:altLabel'), Literal(s, lang=lang))

    def add_description(self, s: str, lang='en'):
        self.add_property(URI('schema:description'), Literal(s, lang=lang))

    def add_statement(self, p: str, v, rank=Rank.Normal, namespace='wd'):
        change_recorder.add((self.node_id, p))
        statement = Statement(self.node_id, rank, self.namespace)
        statement.add_value(p, v, namespace)
        statement.add_property(URI('http://www.isi.edu/etk/createdBy'), self.creator)
        namespace = '' if namespace == 'wd' else namespace
        self.add_property(URI(namespace + 'p:' + p), statement)
        return statement


class NSItem(Entity, Item):
    def __init__(self, s: str, creator='http://www.isi.edu/datamart', namespace='wd'):
        Entity.__init__(self, s, creator, namespace)
        Item.__init__(self, s, namespace)
        self.add_property(URI('rdf:type'), URI('wikibase:Item'))


class NSProperty(Entity, Property):
    def __init__(self, s: str, property_type, creator='http://www.isi.edu/datamart', namespace='wd'):
        Entity.__init__(self, s, creator, namespace)
        Property.__init__(self, s, namespace)
        self.add_property(URI('rdf:type'), URI('wikibase:Property'))
        type_uri = property_type if not isinstance(property_type, Datatype) else Datatype(property_type)
        self.add_property(URI('wikibase:propertyType'), type_uri.type)

        self.add_property(URI('wikibase:directClaim'), URI(namespace + 't:'+s))
        self.add_property(URI('wikibase:directClaimNormalized'), URI(namespace + 'tn:'+s))
        self.add_property(URI('wikibase:novalue'), URI(namespace + 'no:'+s))

        namespace = '' if namespace == 'wd' else namespace
        self.add_property(URI('wikibase:claim'), URI(namespace + 'p:'+s))
        self.add_property(URI('wikibase:statementProperty'), URI(namespace + 'ps:'+s))
        self.add_property(URI('wikibase:statementValue'), URI(namespace + 'psv:'+s))
        self.add_property(URI('wikibase:statementValueNormalized'), URI(namespace + 'psn:'+s))
        self.add_property(URI('wikibase:qualifier'), URI(namespace + 'pq:'+s))
        self.add_property(URI('wikibase:qualifierValue'), URI(namespace + 'pqv:'+s))
        self.add_property(URI('wikibase:qualifierValueNormalized'), URI(namespace + 'pqn:'+s))
        self.add_property(URI('wikibase:reference'), URI(namespace + 'pr:'+s))
        self.add_property(URI('wikibase:referenceValue'), URI(namespace + 'prv:'+s))
        self.add_property(URI('wikibase:referenceValueNormalized'), URI(namespace + 'prn:'+s))


class WDItem(Entity, Item):
    def __init__(self, s: str, creator='http://www.isi.edu/datamart'):
        warnings.warn('WDItem(s) is discarded. Use NSItem(s, namespace=\'wd\') instead.')
        Entity.__init__(self, s, creator)
        Item.__init__(self, s)
        self.add_property(URI('rdf:type'), URI('wikibase:Item'))


class WDProperty(Entity, Property):
    def __init__(self, s: str, property_type, creator='http://www.isi.edu/datamart'):
        warnings.warn('WDProperty(s, type) is discarded. Use NSProperty(s, type, namespace=\'wd\') instead.')
        Entity.__init__(self, s, creator)
        Property.__init__(self, s)
        self.add_property(URI('rdf:type'), URI('wikibase:Property'))
        type_uri = property_type if not isinstance(property_type, Datatype) else Datatype(property_type)
        self.add_property(URI('wikibase:propertyType'), type_uri.type)

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
