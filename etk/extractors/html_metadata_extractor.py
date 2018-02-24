from typing import List
from etk.extractor import Extractor
from etk.etk_extraction import Extraction, Extractable


class HTMLMetadataExtractor(Extractor):
    """
    Extracts microdata, JSON-LD and RDFa from HTML pages
    """

    def __init__(self):
        "consider parameterizing as in extruct, to select only specific types of metadata."
        pass

    @property
    def input_type(self):
        """
        The type of input that an extractor wants
        Returns: HTML text
        """
        return self.InputType.TEXT

    # @property
    def name(self):
        return "HTML metadata extractor"

    # @property
    def category(self):
        return "HTML extractor"

    def extract(self, extractables: List[Extractable]) -> List[Extraction]:
        """
        Uses https://github.com/scrapinghub/extruct to extract metadata from HTML pages
        Args:
            extractables (List[Extractable]): each extractable is expected to contain an HTML string.

        Returns: List[Extraction], where each extraction contains the dict returned by extruct

        """
        pass
