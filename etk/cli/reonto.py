import rdflib
from rdflib.plugins.sparql import prepareQuery
import argparse
import warnings


def read_input_file(f, chunk_size):
    i = 0
    buff = ''

    for line in f:

        if i >= chunk_size:
            yield buff

            i = 0
            buff = ''

        buff += line + '\n'
        i += 1

    yield buff


def add_arguments(parser):
    """
    Parse arguments
    Args:
        parser (argparse.ArgumentParser)
    """
    parser.description = 'Triple re-ontologization'
    parser.add_argument('-i', '--input-file', type=argparse.FileType('r'), dest='input_file')
    parser.add_argument('--input-type', action='store', dest='input_type', default='nt')
    parser.add_argument('-o', '--output-file', type=argparse.FileType('w'), dest='output_file')
    parser.add_argument('--output-type', action='store', dest='output_type', default='nt')
    parser.add_argument('-q', '--query-file', type=argparse.FileType('r'), dest='query_file')
    parser.add_argument('-s', '--chunk-size', action='store', dest='chunk_size', default=1000)


def run(args):
    """
    Args:
        args (argparse.Namespace)
    """

    with warnings.catch_warnings():
        warnings.simplefilter('ignore')

        query = prepareQuery(args.query_file.read())
        # query = args.query_file.read()

        for data in read_input_file(args.input_file, args.chunk_size):
            g = rdflib.Graph()
            g.parse(data=data, format=args.input_type)
            res = g.query(query)
            res = res.serialize(format=args.output_type)
            if res == b'\n':
                continue
            args.output_file.write(res.decode('utf-8'))
