from enum import Enum, auto
from typing import List
from etk.extraction import Extraction


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


class Extractor(object):
    """
    All extractors extend this abstract class.
    """

    def __init__(self, input_type: InputType = InputType.TEXT, category: str = None, name: str = None):
        self._category = category
        self._name = name
        self._input_type = input_type

    @property
    def input_type(self) -> InputType:
        """
        The type of input that an extractor wants
        Returns: InputType
        """
        return self._input_type

    @property
    def name(self) -> str:
        """
        The name of an extractor shown to users.
        Different instances ought to have different names, e.g., a glossary extractor for city_name could have
        name "city name extractor".

        Returns: string, the name of an extractor.
        """
        return self._name

    @property
    def category(self) -> str:
        """
        Identifies a whole category of extractors, all instances should have the same category and
        different names.

        Returns: string, a label to identify the category of an extractor.
        """
        return self._category

    def extract(self, *input_value, **configs) -> List[Extraction]:
        """

        Args:
            input_value (): some extractors may want multiple arguments, for example, to
            concatenate them together
            configs (): any configs/options of extractors

        Returns: list of extracted data, which can be any Python object
        """
        pass
