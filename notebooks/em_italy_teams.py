import json
import requests
import jsonpath_ng.ext as jex
import re

from etk.etk import ETK
from etk.document import Document
from etk.etk_module import ETKModule
from etk.knowledge_graph_schema import KGSchema
from etk.extractors.table_extractor import TableExtractor
from etk.extractors.glossary_extractor import GlossaryExtractor


class ItalyTeamsModule(ETKModule):
    def __init__(self, etk):
        ETKModule.__init__(self, etk)
        self.my_table_extractor = TableExtractor()
        self.etk.parser = jex.parse
        file_name = '${GLOSSARY_PATH}/cities_ppl_25000.json'
        file = open(file_name, 'r')
        self.city_dataset = json.loads(file.read())
        file.close()
        self.city_list = list(self.city_dataset.keys())

        self.my_glossary_extractor = GlossaryExtractor(glossary=self.city_list, extractor_name='tutorial_glossary',
                                                       tokenizer=etk.default_tokenizer, ngrams=3,
                                                       case_sensitive=False)

    def process_document(self, cdr_doc: Document):
        new_docs = list()
        doc_json = cdr_doc.cdr_document

        if 'raw_content' in doc_json and doc_json['raw_content'].strip() != '':
            tables_in_page = self.my_table_extractor.extract(
                doc_json['raw_content'])[:14]
            for table in tables_in_page:

                # skipping the first row, the heading
                for row in table.value['rows'][1:]:
                    doc = self.etk.create_document(row)
                    all_json_path = '$.cells[0:4].text'
                    row_values = doc.select_segments(all_json_path)
                    # add the information we extracted in the knowledge graph of the doc.
                    doc.kg.add_value('team', value=row_values[0].value)
                    doc.kg.add_value('city_name', value=row_values[1].value)
                    doc.kg.add_value('stadium', value=row_values[2].value)
                    capacity_split = re.split(' |,', row_values[3].value)
                    if capacity_split[-1] != '':
                        capacity = int(capacity_split[-2] + capacity_split[-1]) if len(capacity_split) > 1 else int(
                            capacity_split[-1])
                        doc.kg.add_value('capacity', value=capacity)

                    city_json_path = '$.cells[1].text'
                    row_values = doc.select_segments(city_json_path)

                    # use the city field of the doc, run the GlossaryExtractor
                    extractions = doc.extract(
                        self.my_glossary_extractor, row_values[0])
                    if extractions:
                        path = '$."' + \
                               extractions[0].value + '"[?(@.country == "Italy")]'
                        jsonpath_expr = jex.parse(path)
                        city_match = jsonpath_expr.find(self.city_dataset)
                        if city_match:
                            # add corresponding values of city_dataset into knowledge graph of the doc
                            for field in city_match[0].value:
                                doc.kg.add_value(
                                    field, value=city_match[0].value[field])
                    new_docs.append(doc)
        return new_docs

    def document_selector(self, doc) -> bool:
        return doc.cdr_document.get("dataset") == "italy_team"


if __name__ == "__main__":
    # url = 'https://en.wikipedia.org/wiki/List_of_football_clubs_in_Italy'

    cdr = json.load(open('./resources/italy_teams.json', mode='r', encoding='utf-8'))
    kg_schema = KGSchema(json.load(open('./resources/master_config.json')))
    etk = ETK(modules=ItalyTeamsModule, kg_schema=kg_schema)
    etk.parser = jex.parse
    cdr_doc = Document(etk, cdr_document=cdr, mime_type='json', url=cdr['url'])
    results = etk.process_ems(cdr_doc)[1:]
    print('Total docs:', len(results))
    print("Sample result:\n")
    print(json.dumps(results[0].value, indent=2))
