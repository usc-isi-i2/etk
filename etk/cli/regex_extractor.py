import warnings
import sys
import argparse

from etk.extractors.regex_extractor import RegexExtractor


def add_arguments(parser):
    """
       Parse arguments
       Args:
           parser (argparse.ArgumentParser)
       """
    parser.description = 'Examples:\n' \
                         'python -m etk regex_extractor pattern /tmp/date.txt\n' \
                         'cat /tmp/date.txt | python -m etk regex_extractor pattern'
    parser.add_argument('pattern', nargs='?', type=str, default=sys.stdin)
    parser.add_argument('input_file', nargs='?', type=argparse.FileType('r'), default=sys.stdin)


def run(args):
    """
    Args:
        args (argparse.Namespace)
    """
    regex_extractor = RegexExtractor(pattern=args.pattern)

    with warnings.catch_warnings():
        warnings.simplefilter('ignore')

        for line in args.input_file:
            extractions = regex_extractor.extract(line)
            for e in extractions:
                print(e.value)