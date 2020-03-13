import sys
import importlib
import pkgutil
from argparse import ArgumentParser, RawDescriptionHelpFormatter

from etk import cli

# module name should NOT starts with '__' (double underscore)
# module name can not be in 'help', '--help', 'h', '-h'.
handlers = [x.name for x in pkgutil.iter_modules(cli.__path__)
                   if not x.name.startswith('__')]

def help_info():
    print('Usage:')
    print('\t', 'etk <command> [options]')
    print('Available commands:')
    print('\t', ', '.join(handlers))
    print('For the help information of a particular command:')
    print('\t', 'etk <command> -h')
    exit()

def cli_entry():
    """
    Usage:
        etk <command> [options]
        python -m etk <command> [options]
    Example:
        etk dummy --test "this is a test"
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
    mod = importlib.import_module('.' + cmd, 'etk.cli')

    # parse arguments
    parser = ArgumentParser(prog='python -m etk {}'.format(cmd), formatter_class=RawDescriptionHelpFormatter)
    mod.add_arguments(parser)
    args = parser.parse_args(args=sub_cmd)

    # run
    mod.run(args)
