from etk.wikidata.entity import WDItem, WDProperty, change_recorder, serialize_change_record
from etk.wikidata.statement import Rank, WDReference
from etk.wikidata.value import Precision, Item, Property, TimeValue, ExternalIdentifier, QuantityValue, StringValue, \
    URLValue, GlobeCoordinate, MonolingualText, Datatype

wiki_namespaces = {
    'wikibase': 'http://wikiba.se/ontology#',
    'wd': 'http://www.wikidata.org/entity/',
    'wdt': 'http://www.wikidata.org/prop/direct/',
    'wdtn': 'http://www.wikidata.org/prop/direct-normalized/',
    'wdno': 'http://www.wikidata.org/prop/novalue/',
    'wds': 'http://www.wikidata.org/entity/statement/',
    'wdv': 'http://www.wikidata.org/value/',
    'wdref': 'http://www.wikidata.org/reference/',
    'p': 'http://www.wikidata.org/prop/',
    'pr': 'http://www.wikidata.org/prop/reference/',
    'prv': 'http://www.wikidata.org/prop/reference/value/',
    'prn': 'http://www.wikidata.org/prop/reference/value-normalized/',
    'ps': 'http://www.wikidata.org/prop/statement/',
    'psv': 'http://www.wikidata.org/prop/statement/value/',
    'psn': 'http://www.wikidata.org/prop/statement/value-normalized/',
    'pq': 'http://www.wikidata.org/prop/qualifier/',
    'pqv': 'http://www.wikidata.org/prop/qualifier/value/',
    'pqn': 'http://www.wikidata.org/prop/qualifier/value-normalized/',
    'prov': 'http://www.w3.org/ns/prov#',
    'skos': 'http://www.w3.org/2004/02/skos/core#',
    'schema': 'http://schema.org/'
}


def create_custom_prefix(doc, custom_dict={}):
    suffix = {'p': '/prop/', 'pq': '/prop/qualifier/', 'pqn': '/prop/qualifier/value-normalized/',
              'pqv': '/prop/qualifier/value/', 'pr': '/prop/reference/',
              'prn': '/prop/reference/value-normalized/', 'prv': '/prop/reference/value/',
              'ps': '/prop/statement/', 'psn': '/prop/statement/value-normalized/',
              'psv': '/prop/statement/value/', '': '/entity/', 'no': '/prop/novalue/',
              'ref': '/reference/', 's': '/entity/statement/', 't': '/prop/direct/',
              'tn': '/prop/direct-normalized/', 'v': '/value/'}

    # bind wikidata and fixed namespaces
    for k, v in wiki_namespaces.items():
        doc.kg.bind(k, v)

    # bind custom namespaces
    for prefix, path in custom_dict.items():
        for k, v in suffix.items():
            doc.kg.bind(prefix + k, path + v)

    return doc
