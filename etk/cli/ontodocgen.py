from etk.ontology_api import Ontology
from etk.ontology_report_generator import OntologyReportGenerator


def add_arguments(parser):
    parser.description = 'Generate HTML report for the input ontology files'
    parser.add_argument('files', nargs='+', help='Input turtle files.')
    parser.add_argument('--no-validation', action='store_false', dest='validation', default=True,
                        help='Don\'t perform domain and range validation.')
    parser.add_argument('-o', '--output', dest='out', default='ontology-doc.html',
                        help='Location of generated HTML report.')
    parser.add_argument('-i', '--include-undefined-classes', action='store_true',
                        dest='include_class', default=False, help='Include those undefined classes '
                                                                  'but referenced by others.')
    parser.add_argument('-t', '--include-turtle', action='store_true', dest='include_turtle',
                        default=False, help='Include turtle related to this entity. NOTE: this may '
                                            'takes longer time.')
    parser.add_argument('-q', '--quiet', action='store_true', dest='quiet', default=False,
                        help='Suppress warning.')
    parser.add_argument('--exclude-warning', action='store_true', dest='exclude_warning',
                        default=False, help='Exclude warning messages in HTML report')
    parser.add_argument('--list-auxiliary', action='store_true', dest='list_auxiliary',
                        default=False, help='Show auxiliary line for list')


def run(args):
    contents = [open(f).read() for f in args.files]
    ontology = Ontology(contents, validation=args.validation, include_undefined_class=args.include_class,
                        quiet=args.quiet)
    doc_content = OntologyReportGenerator(ontology).generate_html_report(include_turtle=args.include_turtle,
                                                                         exclude_warning=args.exclude_warning,
                                                                         list_auxiliary_line=args.list_auxiliary)

    with open(args.out, "w") as f:
        f.write(doc_content)

