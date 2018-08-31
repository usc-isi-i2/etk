from rdflib.graph import Graph, ConjunctiveGraph, Dataset, URIRef
from rdflib.plugins.sparql import prepareQuery
import argparse
import warnings
import os


def read_by_chunk(f, chunk_size):
    i = 0
    buff = ''

    for line in f:
        if chunk_size != 0:
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
    parser.add_argument('-s', '--chunk-size', action='store', dest='chunk_size', default=0)
    parser.add_argument('-g', '--graph', type=argparse.FileType('r'), dest='graphs', nargs='+', default=[])


def run(args):
    """
    Args:
        args (argparse.Namespace)
    """

    with warnings.catch_warnings():
        warnings.simplefilter('ignore')

        query = prepareQuery(args.query_file.read())

        ds = Dataset()
        for f in args.graphs:
            g = Graph(identifier=os.path.basename(f.name))
            g.parse(data=f.read(), format='n3')
            ds.add_graph(g)

        for data in read_by_chunk(args.input_file, int(args.chunk_size)):
            g = Graph(identifier='data')
            g.parse(data=data, format=args.input_type)
            ds.add_graph(g)
            res = ds.query(query)
            res = res.serialize(format=args.output_type)
            if res == b'\n':
                continue
            args.output_file.write(res.decode('utf-8'))
            ds.remove_graph(g)
