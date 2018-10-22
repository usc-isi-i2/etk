import warnings
import sys
import argparse

from etk.extractors.html_content_extractor import HTMLContentExtractor


def add_arguments(parser):
    """
       Parse arguments
       Args:
           parser (argparse.ArgumentParser)
       """
    parser.description = 'Examples:\n' \
                         'python -m etk html_content_extractor /tmp/input.html\n' \
                         'cat /tmp/input.html | python -m etk html_content_extractor'
    parser.add_argument('input_file', nargs='?', type=argparse.FileType('r'), default=sys.stdin)


def run(args):
    """
    Args:
        args (argparse.Namespace)
    """
    html_content_extractor = HTMLContentExtractor()

    with warnings.catch_warnings():
        warnings.simplefilter('ignore')

        extractions = html_content_extractor.extract(html_text=args.input_file)
        for e in extractions:
            print(e.value)
