import json, os, sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from etk.etk import ETK
from etk.etk_module import ETKModule
from etk.extractors.excel_extractor import ExcelExtractor
from etk.knowledge_graph_schema import KGSchema
from etk.utilities import Utility
import re
import sys


class ElicitWestAmericanFoodModule(ETKModule):
    """
    Abstract class for extraction module
    """
    def __init__(self, etk):
        ETKModule.__init__(self, etk)
        self.ee = ExcelExtractor()

    def document_selector(self, doc):
        return 'file_path' in doc.cdr_document

    def process_document(self, doc):
        # extraction
        variables = {
            'value': '$col,$row',
            'food_name_in_english': '$B,$row',
            'food_name_in_french': '$C,$row',
            'scientific_name': '$D,$row',
            'code': '$A,$row',
            'source': '$E,$row',
            'nutrition': '$col,$2',
            'row': '$row',
            'col': '$col'
        }

        raw_extractions = self.ee.extract(doc.cdr_document['file_path'], 'USERDATABASE', ['F,3', 'AG,971'], variables)

        # post processing
        re_code = re.compile(r'^[0-9]{2}_[0-9]{3}$')
        re_value_in_bracket = re.compile(r'^.*\((.*)\)')
        re_value_in_square_bracket = re.compile(r'^.*\[(.*)\]')
        extracted_docs = []
        for e in raw_extractions:
            code = e['code'].strip()
            if not re_code.match(code):
                continue

            in_bracket_unit = re_value_in_bracket.search(e['nutrition'])
            unit = '' if not in_bracket_unit else in_bracket_unit.groups(1)[0]

            # parse value
            value = e['value']
            if e['nutrition'] == 'Energy (kcal) kJ':
                in_bracket_value = re_value_in_bracket.search(e['value'])
                value = in_bracket_value.groups(1)[0]
            elif isinstance(value, str):
                value = value.strip()
                in_square_bracket_value = re_value_in_square_bracket.search(e['value'])
                if in_square_bracket_value:
                    value = in_square_bracket_value.groups(1)[0]

                # if it's a range, get the lower bound
                dash_pos = value.find('-')
                if dash_pos != -1:
                    value = value[:dash_pos]
            try:
                value = float(value)
            except:
                value = 0.0


            extracted_doc = {
                'tld': '',
                'website': '',
                'type': 'factoid',
                'factoid': {
                    'value': value,
                    'unit': unit,
                    'food_name_in_english': e['food_name_in_english'],
                    'food_name_in_french': e['food_name_in_french'],
                    'scientific_name': e['scientific_name'],
                    'source': e['source'],
                    'code': code,
                    'nutrition': e['nutrition'],
                    'metadata': {
                        'file_name': os.path.basename(doc.cdr_document['file_path']),
                        'sheet_name': 'USERDATABASE',
                        'row': str(e['row']),
                        'col': str(e['col'])
                    },
                    'identifier_key': 'code',
                    'identifier_value': code
                }
            }
            extracted_doc['doc_id'] = Utility.create_doc_id_from_json(extracted_doc)
            extracted_doc = etk.create_document(extracted_doc)

            # build kg
            extracted_doc.kg.add_value('metadata__unit', json_path='$.factoid.unit')
            extracted_doc.kg.add_value('metadata__property_type',
                value=['http://ontology.causeex.com/ontology/odps/TimeSeriesAndMeasurements#Nutrition'])
            extracted_doc.kg.add_value('metadata__reported_value',
                value=['http://ontology.causeex.com/ontology/odps/TimeSeriesAndMeasurements#ReportedValue'])
            extracted_doc.kg.add_value('provenance_col', json_path='$.factoid.metadata.col')
            extracted_doc.kg.add_value('provenance_row', json_path='$.factoid.metadata.row')
            extracted_doc.kg.add_value('provenance_filename', json_path='$.factoid.metadata.file_name')
            extracted_doc.kg.add_value('provenance_sheet', json_path='$.factoid.metadata.sheet_name')
            extracted_doc.kg.add_value('value', json_path='$.factoid.value')
            extracted_doc.kg.add_value('type', json_path='$.factoid.type')
            extracted_doc.kg.add_value('identifier_key', json_path='$.factoid.identifier_key')
            extracted_doc.kg.add_value('identifier_value', json_path='$.factoid.identifier_value')

            extracted_docs.append(extracted_doc)

        return extracted_docs


if __name__ == "__main__":
    # elicit_alignment/m9/datasets/orig/structured/west_african_food_composition/example/
    dir_path = sys.argv[1]
    file_name = 'West African Food Composition.xls'
    input_path = os.path.join(dir_path, file_name)
    output_path = os.path.join(dir_path, file_name + '.jl')

    kg_schema = KGSchema(json.load(open('master_config.json')))
    etk = ETK(modules=ElicitWestAmericanFoodModule, kg_schema=kg_schema)
    doc = etk.create_document({'file_path': input_path})

    docs = etk.process_ems(doc)
    with open(output_path, 'w') as f:
        for i in range(1, len(docs)): # ignore the first
            f.write(json.dumps(docs[i].value) + '\n')
