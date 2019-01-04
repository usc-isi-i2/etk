import warnings
import sys
import argparse

from etk.extractors.cve_extractor import CVEExtractor

cve_extractor = CVEExtractor()


def add_arguments(parser):
    """
    Parse arguments
    Args:
        parser (argparse.ArgumentParser)
    """
    parser.description = 'Examples:\n' \
                         'python -m etk bitcoin_address_extractor /tmp/input.txt\n' \
                         'cat /tmp/input.txt | python -m etk bitcoin_address_extractor'
    parser.add_argument('input_file', nargs='?', type=argparse.FileType('r'), default=sys.stdin)


def run(args):
    """
    Args:
        args (argparse.Namespace)
    """
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')

        for line in args.input_file:
            extractions = cve_extractor.extract(line)
            for e in extractions:
                print(e.value)
