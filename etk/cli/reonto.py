from rdflib.graph import Graph, ConjunctiveGraph, Dataset, URIRef
from rdflib.plugins.sparql import prepareQuery
from rdflib.plugins.sparql.processor import SPARQLResult
import argparse
import warnings
import os
import hashlib


def read_by_chunk(f, chunk_size):
    i = 0
    # buff_prev + buff = slide window
    buff_prev = ''
    buff = ''

    for line in f:
        if chunk_size != 0:
            if i >= chunk_size:
                yield buff_prev + buff
                i = 0
                buff_prev = buff
                buff = ''
        buff += line + '\n'
        i += 1
    yield buff_prev + buff


def generate_index(triple):
    s, p, o = triple
    content = '{} {} {}'.format(str(s), str(p), str(o))
    return hashlib.md5(content.encode('utf-8')).hexdigest()


def add_arguments(parser):
    """
    Parse arguments
    Args:
        parser (argparse.ArgumentParser)
    """
    parser.description = 'Triple re-ontologization'
    parser.add_argument('-i', '--input-file', type=argparse.FileType('r'), dest='input_file')
    parser.add_argument('--input-type', action='store', dest='input_type', default='nt')
    parser.add_argument('-o', '--output-file', type=argparse.FileType('wb'), dest='output_file')
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

        res_indices_prev = set()  # de-duplication
        res_indices = set()

        # create sub graphs
        for f in args.graphs:
            g = Graph(identifier=os.path.basename(f.name))
            g.parse(data=f.read(), format='n3')
            ds.add_graph(g)

        # create and query data graph
        for data in read_by_chunk(args.input_file, int(args.chunk_size)):
            g = Graph(identifier='data')
            g.parse(data=data, format=args.input_type)
            ds.add_graph(g)
            res = ds.query(query)

            dedup_res_graph = Graph()
            if len(res) != 0:
                for r in res:
                    tid = generate_index(r)
                    res_indices.add(tid)
                    if tid in res_indices_prev:  # duplicated
                        continue
                    dedup_res_graph.add(r)

                if len(dedup_res_graph) > 0:
                    ret = dedup_res_graph.serialize(format=args.output_type, encoding='utf-8')
                    args.output_file.write(ret)

            ds.remove_graph(g)
            res_indices_prev = res_indices
            res_indices = set()
