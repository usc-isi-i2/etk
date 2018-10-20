from typing import List
from etk.extraction import Extraction
from etk.extractor import Extractor, InputType

from enum import Enum, auto
from langid import classify
from langdetect import detect


class LanguageIdentificationExtractor(Extractor):
    """
    **Description**
        Identify the language used in text, returning the identifier language using ISO 639-1 codes

        Uses two libraries:
        - https://github.com/davidjurgens/equilid
        - https://github.com/saffsd/langid.py

        TODO: define Enum to select which method to use.
        TODO: define dictionary to translate ISO 639-3 to ISO 639-1 codes
        https://en.wikipedia.org/wiki/List_of_ISO_639-1_codes, perhaps there is an online source that has this

    Examples:
        ::

            language_identification_extractor = LanguageIdentificationExtractor()
            language_identification_extractor.extract(text=input_stri,
                                                    method=IdentificationTool.LANGDETECT.name)

    """

    def __init__(self):
        Extractor.__init__(self,
                           input_type=InputType.TEXT,
                           category="Text extractor",
                           name="Language Identification")

    def extract(self, text: str, method: str) -> List[Extraction]:
        """

        Args:
            text (str): any text, can contain HTML
            method (Enum[IdentificationTool.LANGID, IdentificationTool.LANGDETECT]): specifies which of the two
            algorithms to use

        Returns:
            List(Extraction): an extraction containing the language code used in the text. Returns the empty list of
            the extractor fails to identify the language in the text.

        """
        if method == IdentificationTool.LANGID.name:
            language = classify(text)[0]
            return [Extraction(value=language, extractor_name=self.name)]

        elif method == IdentificationTool.LANGDETECT.name:
            try:
                language = detect(text)
            except:
                language = 'unknown'

            if language == 'unknown':
                return list()
            else:
                return [Extraction(value=language, extractor_name=self.name)]

        else:
            return list()


class IdentificationTool(Enum):
    LANGID = auto()
    LANGDETECT = auto()
