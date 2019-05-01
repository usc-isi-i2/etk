from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore


class NodeProperty:
    def __init__(self, node, property_, endpoint: SPARQLUpdateStore, dryrun=False):
        self.node = node
        self.prop = property_
        self.endpoint = endpoint
        self.dryrun = dryrun

    def ask(self, query):
        for ans in self.endpoint.query(query):
            return ans

    def update(self, query):
        if self.dryrun:
            action = 'INSERT' if 'INSERT' in query else 'DELETE'
            # address empty result with CONSTRUCT
            # https://github.com/RDFLib/rdflib/issues/775
            if self.ask('ASK ' + query[query.find('WHERE')+5:]):
                res = self.endpoint.query(query.replace(action, 'CONSTRUCT'))
                print('### About to {} following triples:'.format(action))
                for row in res:
                    print(' '.join(e.n3() for e in row))
        else:
            self.endpoint.update(query)

    def has_truthy_preferred_rank(self):
        query = 'ask { wd:%(node)s p:%(prop)s [ a wikibase:BestRank ; wikibase:rank wikibase:PreferredRank ] }'
        query = query % {'node': self.node, 'prop': self.prop}
        return self.ask(query)

    def has_preferred_rank(self):
        query = 'ask { wd:%(node)s p:%(prop)s/wikibase:rank wikibase:PreferredRank }'
        query = query % {'node': self.node, 'prop': self.prop}
        return self.ask(query)

    def insert_other_rank_statements(self, rank):
        query = '''
          INSERT {
            ?statement a wikibase:BestRank .
            wd:%(node)s wdt:%(prop)s ?value .
            wd:%(node)s wdtn:%(prop)s ?normalValue
          } WHERE {
            ?statement ^p:%(prop)s wd:%(node)s ;
                        wikibase:rank wikibase:%(rank)s ;
                        ps:%(prop)s ?value .
            OPTIONAL { ?statement psn:%(prop)s ?normalValue }
            FILTER NOT EXISTS { ?statement a wikibase:BestRank }
          }
        ''' % {'node': self.node, 'prop': self.prop, 'rank': rank}
        self.update(query)

    def delete_truthy_normal_rank(self):
        query = '''
          DELETE {
            ?statement a wikibase:BestRank .
            wd:%(node)s wdt:%(prop)s ?value .
            wd:%(node)s wdtn:%(prop)s ?normalValue
          } WHERE {
            ?statement a wikibase:BestRank ;
                       ^p:%(prop)s wd:%(node)s ;
                       wikibase:rank wikibase:NormalRank ;
                       ps:%(prop)s ?value .
            OPTIONAL { ?statement psn:%(prop)s ?normalValue }
          }
        ''' % {'node': self.node, 'prop': self.prop}
        self.update(query)


class TruthyUpdater:
    def __init__(self, endpoint):
        self.endpoint = SPARQLUpdateStore(endpoint)

    def update(self, node, property_, dryrun=False):
        np = NodeProperty(node, property_, self.endpoint, dryrun)
        if np.has_truthy_preferred_rank():
            np.insert_other_rank_statements('PreferredRank')
        elif np.has_preferred_rank():
            np.delete_truthy_normal_rank()
            np.insert_other_rank_statements('PreferredRank')
        else:
            np.insert_other_rank_statements('NormalRank')


if __name__ == '__main__':
    np = NodeProperty('Q42', 'P69', SPARQLUpdateStore('https://query.wikidata.org/sparql'), True)
    print(np.has_truthy_preferred_rank())
    print(np.has_preferred_rank())
    np.delete_truthy_normal_rank()
    np.insert_other_rank_statements('NormalRank')
