import warnings
import sys
import argparse

from etk.extractors.date_extractor import DateExtractor

date_extractor = DateExtractor()


def add_arguments(parser):
    """
    Parse arguments
    Args:
        parser (argparse.ArgumentParser)
    """
    parser.description = 'Examples:\n' \
                         'python -m etk date_extractor /tmp/date.txt\n' \
                         'cat /tmp/date.txt | python -m etk date_extractor'
    parser.add_argument('input_file', nargs='?', type=argparse.FileType('r'), default=sys.stdin)


def run(args):
    """
    Args:
        args (argparse.Namespace)
    """
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')

        for line in args.input_file:
            extractions = date_extractor.extract(line)
            for e in extractions:
                print(e.value)
