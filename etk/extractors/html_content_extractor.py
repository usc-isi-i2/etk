from typing import List
from enum import Enum, auto
from etk.extractor import Extractor, InputType
from etk.etk_extraction import Extraction, Extractable


class Strategy(Enum):
    """
    ALL_TEXT: return all visible text in an HTML page
    MAIN_CONTENT_STRICT: MAIN_CONTENT_STRICT: return the main content of the page without boiler plate
    MAIN_CONTENT_RELAXED: variant of MAIN_CONTENT_STRICT with less strict rules
    """
    ALL_TEXT = auto()
    MAIN_CONTENT_STRICT = auto()
    MAIN_CONTENT_RELAXED = auto()


class HTMLContentExtractor(Extractor):
    """
    Extracts text from HTML pages.

    Uses readability and BeautifulSoup
    """

    def __init__(self):
        Extractor.__init__(self,
                           input_type=InputType.HTML,
                           category="HTML extractor",
                           name="HTML content extractor")

    def extract(self, html_text: str, strategy: Strategy = Strategy.ALL_TEXT) \
            -> List[str]:
        """
        Extracts text from an HTML page using a variety of strategies

        Args:
            html_text ():
            strategy ():

        Returns: a list of str, typically a singleton list with the extracted text
        """
        pass
