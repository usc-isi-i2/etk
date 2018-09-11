"""
Example CLI module
"""


def add_arguments(parser):
    """
    Parse arguments
    Args:
        parser (argparse.ArgumentParser)
    """
    parser.add_argument("-t", "--test", action="store", type=str, dest="test_string")


def run(args):
    """
    Args:
        args (argparse.Namespace)
    """
    print(args.test_string or 'no test string provided')
