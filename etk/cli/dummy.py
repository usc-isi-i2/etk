def add_arguments(parser):
    parser.add_argument("-t", "--test", action="store", type=str, dest="test_string")
    return parser


def run(args):
    print(args.test_string)