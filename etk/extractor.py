from enum import Enum, auto
from typing import List, Dict


class Extractor(object):
    """
    All extractors extend this abstract class.
    """

    class InputType(Enum):
        """
        TEXT: value must be a string
        HTML: the value is HTML text
        TOKENS: value must first be tokenized
        OBJECT: value can be anything
        """
        TEXT = auto()
        HTML = auto()
        TOKENS = auto()
        OBJECT = auto()

    @property
    def input_type(self) -> InputType:
        """
        The type of input that an extractor wants
        Returns: InputType
        """
        return self.InputType.TEXT

    @property
    def name(self) -> str:
        """
        The name of an extractor shown to users.
        Different instances ought to have different names, e.g., a glossary extractor for city_name could have
        name "city name extractor".

        Returns: string, the name of an extractor.
        """
        pass

    @property
    def category(self) -> str:
        """
        Identifies a whole category of extractors, all instances should have the same category and
        different names.

        Returns: string, a label to identify the category of an extractor.
        """
        pass

    def extract(self, *input_value) -> List[Dict]:
        """

        Args:
            input_value (): some extractors may want multiple arguments, for example, to
            concatenate them together

        Returns: list of extracted_result
        """
        pass
