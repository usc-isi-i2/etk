import warnings
import sys
import argparse

from etk.extractors.cryptographic_hash_extractor import CryptographicHashExtractor

cryptographic_hash_extractor = CryptographicHashExtractor()


def add_arguments(parser):
    """
    Parse arguments
    Args:
        parser (argparse.ArgumentParser)
    """
    parser.description = 'Examples:\n' \
                         'python -m etk cryptographic_hash_extractor /tmp/input.txt\n' \
                         'cat /tmp/input.txt | python -m etk cryptographic_hash_extractor'
    parser.add_argument('input_file', nargs='?', type=argparse.FileType('r'), default=sys.stdin)


def run(args):
    """
    Args:
        args (argparse.Namespace)
    """
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')

        for line in args.input_file:
            extractions = cryptographic_hash_extractor.extract(line)
            for e in extractions:
                print(e.value)
