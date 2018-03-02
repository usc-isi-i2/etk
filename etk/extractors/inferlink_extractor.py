from typing import List
from etk.extractor import Extractor, InputType
from etk.etk_extraction import Extraction


class InferlinkRule(object):
    """
    Wrapper of a single rule.
    """

    @property
    def name(self) -> str:
        pass

    def apply(self, html_text: str) -> str:
        pass


class InferlinkRuleSet(object):
    """
    Wrapper class on an inferlink JSON to provide a convenient API to work with rules.
    """

    def __init__(self, rule_set):
        pass

    @property
    def rules(self) -> List[InferlinkRule]:
        pass

    @staticmethod
    def load_rules_file(file_name: str) -> 'InferlinkRuleSet':
        pass


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
            html_text ():
            threshold ():

        Returns:

        """
        result = list()
        for rule in self.rule_set.rules:
            value = rule.apply(html_text)
            if value is not None:
                # note the addition of a new tag argument to Extraction
                result.append(Extraction(value, tag=rule.name))

        # Test whether the fraction of extractions meets the desired threshold
        if len(result) > len(self.rules) + threshold:
            return result
        else:
            return list()