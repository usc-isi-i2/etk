from typing import List
from etk.extractor import Extractor
from etk.etk_extraction import Extraction, Extractable


class HTMLMetadataExtractor(Extractor):
    """
    Extracts META, microdata, JSON-LD and RDFa from HTML pages.

    Uses https://stackoverflow.com/questions/36768068/get-meta-tag-content-property-with-beautifulsoup-and-python to
    extract the META tags

    Uses https://github.com/scrapinghub/extruct to extract metadata from HTML pages
    """

    def __init__(self):
        pass

    @property
    def input_type(self):
        """
        The type of input that an extractor wants
        Returns: HTML text
        """
        return self.InputType.HTML

    # @property
    def name(self):
        return "HTML metadata extractor"

    # @property
    def category(self):
        return "HTML extractor"

    def extract(self, extractables: List[Extractable],
                extract_title: bool = False,
                extract_meta: bool = False,
                extract_microdata: bool = False,
                extract_json_ld: bool = False,
                extract_rdfa: bool = False) \
            -> List[Extraction]:
        """

        Args:
            extractables ():
            extract_title (): extract the <title> tag from the HTML page, return as { "title": "..." }
            extract_meta (): extract the meta tags, return as { "meta": { "author": "...", ...}}
            extract_microdata (): extract microdata, returns as { "microdata": [...] }
            extract_json_ld (): extract JSON-LD, return as { "json-ld": [...] }
            extract_rdfa (): extract rdfa, returns as { "rdfa": [...] }

        Returns: List[Extraction], where each extraction contains a dict with each type of metadata.

        """
        pass
