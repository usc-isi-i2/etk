from typing import List
from enum import Enum, auto
from etk.extractor import Extractor
from etk.etk_extraction import Extraction, Extractable


class HTMLMetadataExtractor(Extractor):
    """
    Extracts text from HTML pages.

    Uses readability and BeautifulSoup
    """

    class Strategy(Enum):
        """
        ALL_TEXT: return all visible text in an HTML page
        MAIN_CONTENT_STRICT: MAIN_CONTENT_STRICT: return the main content of the page without boiler plate
        MAIN_CONTENT_RELAXED: variant of MAIN_CONTENT_STRICT with less strict rules
        """
        ALL_TEXT = auto()
        MAIN_CONTENT_STRICT = auto()
        MAIN_CONTENT_RELAXED = auto()

    def __init__(self):
        pass

    @property
    def input_type(self):
        return self.InputType.HTML

    # @property
    def name(self):
        return "HTML content extractor"

    # @property
    def category(self):
        return "HTML extractor"

    def extract(self, extractables: List[Extractable], strategy: Strategy = Strategy.ALL_TEXT) \
            -> List[Extraction]:

        pass
