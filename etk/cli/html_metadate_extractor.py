import warnings
import sys
import argparse

from etk.extractors.html_metadata_extractor import HTMLMetadataExtractor


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
    html_metadata_extractor = HTMLMetadataExtractor()

    with warnings.catch_warnings():
        warnings.simplefilter('ignore')

        extractions = html_metadata_extractor.extract(html_text=args.input_file)
        for e in extractions:
            print(e.value)
