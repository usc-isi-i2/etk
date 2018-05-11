import pprint
import os
import sys
import codecs
import json

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from etk.etk import ETK
from etk.etk_module import ETKModule
from etk.csv_processor import CsvProcessor
from optparse import OptionParser


class CsvETKModule(ETKModule):
    """
       Abstract class for extraction module
       """

    def __init__(self, etk):
        ETKModule.__init__(self, etk)

    def process_document(self, doc):
        pass


def parse_gdelt_mapping(parsed_docs):
    new_parsed_doc = dict()
    for parsed_doc in parsed_docs:

        new_doc = dict()
        for key in parsed_doc.keys():
            if '+' not in parsed_doc[key]:
                new_doc[key] = parsed_doc[key]
            else:
                values = convert_plus_to_list(parsed_doc[key])
                new_doc[key] = values
        new_doc.pop('file_name')
        new_doc.pop('dataset')
        new_parsed_doc[parsed_doc['CAMEO code']] = new_doc
    return new_parsed_doc


def convert_plus_to_list(value):
    vals = value.split('+')
    vals = [val.strip() for val in vals]
    for val in vals:
        if 'has_topic' in val:
            vals.remove(val)
    return vals


if __name__ == "__main__":
    parser = OptionParser()

    (c_options, args) = parser.parse_args()
    input_path = args[0]
    output_path = args[1]

    etk = ETK(modules=CsvETKModule)
    csv_processor = CsvProcessor(etk=etk, heading_row=1)

    data_set = 'elicit_gdelt_mapping'
    parsed_docs = [doc.cdr_document for doc in
                   csv_processor.tabular_extractor(filename=input_path, dataset='elicit_mapping')]

    open(output_path, 'w').write(json.dumps(parse_gdelt_mapping(parsed_docs), indent=2))
