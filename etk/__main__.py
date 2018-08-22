import sys
import os
import importlib
from argparse import ArgumentParser, RawDescriptionHelpFormatter

# module name should NOT starts with '__' (double underscore)
# module name can not be in 'help', '--help', 'h', '-h'.
dir_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'cli')
handlers = list(filter(lambda x: not x.startswith('__'),
                       [os.path.splitext(fname)[0] for fname in os.listdir(dir_path)]))


def help_info():
    print('Usage:')
    print('\t', 'python -m etk <command> [options]')
    print('Available commands:')
    print('\t', ', '.join(handlers))
    print('For the help information of a particular command:')
    print('\t', 'python -m etk <command> -h')
    exit()

if __name__ == '__main__':
    """
    Usage:
        python -m etk <command> [options]
    Example:
        python -m etk dummy --test "this is a test"
    """
    if len(sys.argv) <= 1:
        print('No command\n')
        help_info()

    cmd = sys.argv[1]
    sub_cmd = sys.argv[2:] if len(sys.argv) >= 3 else []

    if cmd in ('help', '--help', 'h', '-h'):
        help_info()

    if cmd not in handlers:
        print('Unknown command\n')
        help_info()

    # load module
    sys.path.append(dir_path)
    mod = importlib.import_module(cmd)

    # parse arguments
    parser = ArgumentParser(prog='python -m etk {}'.format(cmd), formatter_class=RawDescriptionHelpFormatter)
    mod.add_arguments(parser)
    args = parser.parse_args(args=sub_cmd)

    # run
    mod.run(args)
