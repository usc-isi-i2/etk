from etk.wikidata.truthy import TruthyUpdater


def add_arguments(parser):
    parser.add_argument('-e', '--endpoint', action='store', type=str, dest='endpoint')
    parser.add_argument('-c', '--creator', default='http://www.isi.edu/datamart', action='store', type=str,
                        dest='creator')
    parser.add_argument('--user', action='store', default=None, type=str, dest='user')
    parser.add_argument('--passwd', action='store', default=None, type=str, dest='passwd')
    parser.add_argument('--dry-run', action='store_true', default=False, dest='dryrun')


def run(args):
    print('Start deleting statements created by {} in endpoint {}'.format(args.creator, args.endpoint))
    if args.dryrun:
        print('Mode: Dryrun')
    tu = TruthyUpdater(args.endpoint, args.dryrun, args.user, args.passwd)
    creator = args.creator

    query = '''
      DELETE {
        ?statement a wikibase:BestRank .
        ?entity ?wdt ?value .
        ?entity ?wdtn ?normalValue .
      } WHERE {
        ?statement <http://www.isi.edu/etk/createdBy> <%s> ;
                   a wikibase:BestRank .
        ?entity ?p ?statement .
        ?statement ?ps ?value .
        ?entity ?wdt ?value .
        FILTER (strStarts(str(?p), "http://www.wikidata.org/prop/") && strStarts(str(?ps), "http://www.wikidata.org/prop/statement/") && strStarts(str(?wdt), "http://www.wikidata.org/prop/direct/"))
        OPTIONAL {
            ?entity ?wdtn ?normalValue .
            ?statement ?psn ?normalValue .
            FILTER (strStarts(str(?psn), "http://www.wikidata.org/prop/statement/value-normalized/") && strStarts(str(?wdtn), "http://www.wikidata.org/prop/direct-normalized/"))
        }
      }
    ''' %  creator
    tu.update(query)


    query = '''
      DELETE {
        ?statement ?p1 ?o .
        ?s ?p2 ?statement
      } WHERE {
        ?statement <http://www.isi.edu/etk/createdBy> <%s> .
        ?statement ?p1 ?o 
        OPTIONAL { ?s ?p2 ?statement }
      }
    ''' % creator
    tu.update(query)

    query = '''
      DELETE {
        ?entity ?p1 ?o .
        ?s ?p2 ?entity
      } WHERE {
        { ?entity a wikibase:Item } UNION { ?entity a wikibase:Property }
        FILTER NOT EXISTS { ?entity ?p [ wikibase:rank ?rank ] }
        ?entity ?p1 ?o .
        OPTIONAL { ?s ?p2 ?entity }
      }
    '''
    tu.update(query)
    print('Deletion complete!')
