import json
from typing import List
from etk.extractor import Extractor, InputType
from etk.extraction import Extraction
from etk.etk_exceptions import ExtractorError
from etk.dependencies.landmark.landmark_extractor.extraction.Landmark import ItemRule, IterationRule, loadRule


class InferlinkRule(object):
    """
    Wrapper of a single rule.
    """

    def __init__(self, rule: dict):
        self._name = rule['name'] if 'name' in rule else ''
        self._value = None
        self._start_char = None
        self._end_char = None
        self._rule = loadRule(rule)

    @property
    def name(self) -> str:
        return self._name

    @property
    def value(self) -> str:
        return self._value

    @property
    def start_char(self) -> int:
        return self._start_char

    @property
    def end_char(self) -> int:
        return self._end_char

    def apply(self, html_text: str):
        extraction = self._rule.apply(html_text)  # a dict with 'rule_id', 'extract', 'begin_index', 'end_index'...
        self._value = extraction['extract'] if 'extract' in extraction and extraction['extract'] != '' else None
        self._start_char = extraction['begin_index'] if 'begin_index' in extraction and extraction[
            'extract'] != -1 else None
        self._end_char = extraction['end_index'] if 'end_index' in extraction and extraction['extract'] != -1 else None


class InferlinkRuleSet(object):
    """
    Wrapper class on an inferlink JSON to provide a convenient API to work with rules.
    """

    def __init__(self, rule_set: List[dict]):
        self._rules = [InferlinkRule(rule) for rule in rule_set]

    @property
    def rules(self) -> List[InferlinkRule]:
        return self._rules

    @staticmethod
    def load_rules_file(file_name: str) -> List[dict]:
        with open(file_name, 'r') as f:
            return json.load(f)['rules']


class InferlinkExtractor(Extractor):
    """
    Extracts segments from an HTML page using rules created by the Inferlink web wrapper.
    """

    def __init__(self, rule_set: InferlinkRuleSet):
        Extractor.__init__(self,
                           input_type=InputType.HTML,
                           category="HTML extractor",
                           name="Inferlink extractor")
        self.rule_set = rule_set

    def extract(self, html_text: str, threshold=0.5) -> List[Extraction]:
        """

        Args:
            html_text (): str of the html page to be extracted
            threshold (): if the ratio of rules that successfully extracted something over all rules \
                    is higher than or equal to the threshold, return the results, else return an empty list

        Returns: a list of Extractions, each extraction includes the extracted value, the rule name, the provenance etc.

        """

        result = list()
        try:
            for rule in self.rule_set.rules:
                rule.apply(html_text)
                value = rule.value
                if value is not None:
                    # note the addition of a new tag argument to Extraction
                    start_char = rule.start_char
                    end_char = rule.end_char
                    result.append(Extraction(value, self.name, start_char=start_char, end_char=end_char, tag=rule.name))

            # Test whether the fraction of extractions meets the desired threshold
            if len(self.rule_set.rules) > 0 and float(len(result)) / len(self.rule_set.rules) >= threshold:
                return result
            else:
                return list()
        except Exception as e:
            raise ExtractorError('Error in extracting landmark %s' % e)
