
from datetime import datetime
import json


class EmBaseGenerator(object):
    def __init__(self, template_path: str='template.txt'):
        with open(template_path, 'r') as f:
            self.template = f.read()

    def generate_em_base(self, master_config: str, output: str='em_base.py') -> None:
        """

        Args:
            master_config: string - file path to a json file containing the configs
            output: string - file path to a python script as the output file

        Returns: None

        """
        configs = json.load(open(master_config, 'r'))
        fields = configs['fields']
        glossary_dicts = configs['glossary_dicts']
        extractors = []
        executions = []
        for f in fields:
            if 'glossaries' in fields[f] and fields[f]['glossaries']:
                glossary_name = fields[f]['glossaries'][0]
                glossary_path = ''
                if glossary_name in glossary_dicts:
                    if 'path' in glossary_dicts[glossary_name]:
                        glossary_path = glossary_dicts[glossary_name]['path']
                if glossary_path:
                    extractors.append('        ' + self.generate_glossary_extractor(f, glossary_path) + '\n')
                    executions.append('            ' + self.generate_execution(f) + '\n')
            elif 'rule_extractor_enabled' in fields[f] and fields[f]['rule_extractor_enabled']:
                extractors.append('        ' + self.generate_spacy_rule_extractor(f) + '\n')
                executions.append('            ' + self.generate_execution(f) + '\n')
        final = self.template.replace('${extractor_list}',
                                      ''.join(extractors)).replace('${execution_list}', ''.join(executions))
        with open(output, 'w') as output_file:
            output_file.write(final)

    @staticmethod
    def generate_execution(field_id: str) -> str:
        template = "for extraction in doc.invoke_extractor(self.{id}_extractor, text): " \
                   "doc.kg.add_value('{id}', extraction.value)"
        return template.format(id=field_id)

    @staticmethod
    def generate_glossary_extractor(field_id: str, glossary_path: str, ngrams: int=2, case_sensitive: bool=False) -> str:
        template = "self.{id}_extractor = GlossaryExtractor(self.etk.load_glossary('{path}'), " \
                   "'{id}_extractor', self.etk.default_tokenizer, case_sensitive={case_sensitive}, ngrams={ngrams})"
        return template.format(id=field_id, path=glossary_path, case_sensitive=str(case_sensitive), ngrams=str(ngrams))

    @staticmethod
    def generate_spacy_rule_extractor(field_id: str) -> str:
        template = "self.{id}_extractor = SpacyRuleExtractor(self.etk.default_nlp, " \
                   "self.etk.load_spacy_rule('./spacy_rules/{id}.json'), '{id}_extractor')"
        return template.format(id=field_id)


ebg = EmBaseGenerator('template.txt')
ebg.generate_em_base('master_config.json', 'test_em_base.py')