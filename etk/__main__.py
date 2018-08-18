import sys
import os
import importlib
from argparse import ArgumentParser

dir_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'cli')
handlers = list(filter(lambda x: not x.startswith('__'),
                       [os.path.splitext(fname)[0] for fname in os.listdir(dir_path)]))

if __name__ == '__main__':
    """
    python -m etk dummy --test "this is a test"
    """
    cmd = sys.argv[1]
    sub_cmd = sys.argv[2:]
    if cmd not in handlers:
        print('Unknown command')
        print('Available commands:', ','.join(handlers))
        exit()

    # load module
    sys.path.append(dir_path)
    mod = importlib.import_module(cmd)

    # parse arguments
    parser = ArgumentParser()
    parser = mod.add_arguments(parser)
    args, _ = parser.parse_known_args(args=sub_cmd)

    # run
    mod.run(args)