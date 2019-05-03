from etk.wikidata.truthy import TruthyUpdater


def add_arguments(parser):
    parser.add_argument('-e', '--endpoint', action='store', type=str, dest='endpoint')
    parser.add_argument('-f', '--input', action='store', type=str, dest='file')
    parser.add_argument('--user', action='store', default=None, type=str, dest='user')
    parser.add_argument('--passwd', action='store', default=None, type=str, dest='passwd')
    parser.add_argument('--dry-run', action='store_true', default=False, dest='dryrun')


def run(args):
    print('Start updating endpoint: {}'.format(args.endpoint))
    if args.dryrun:
        print('Mode: Dryrun')
    tu = TruthyUpdater(args.endpoint, args.dryrun, args.user, args.passwd)
    np_list = []
    with open(args.file) as f:
        for l in f.readlines():
            if not l: continue
            node, prop = l.strip().split('\t')
            np_list.append((node, prop))
    tu.build_truthy(np_list)
    print('Update finished!')
