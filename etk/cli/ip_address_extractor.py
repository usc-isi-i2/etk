import warnings
import sys
import argparse

from etk.extractors.ip_address_extractor import IPAddressExtractor

ip_address_extractor = IPAddressExtractor()


def add_arguments(parser):
    """
    Parse arguments
    Args:
        parser (argparse.ArgumentParser)
    """
    parser.description = 'Examples:\n' \
                         'python -m etk ip_address_extractor /tmp/input.txt\n' \
                         'cat /tmp/input.txt | python -m etk ip_address_extractor'
    parser.add_argument('input_file', nargs='?', type=argparse.FileType('r'), default=sys.stdin)


def run(args):
    """
    Args:
        args (argparse.Namespace)
    """
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')

        for line in args.input_file:
            extractions = ip_address_extractor.extract(line)
            for e in extractions:
                print(e.value)
