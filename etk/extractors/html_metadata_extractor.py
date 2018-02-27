from typing import List
from etk.extractor import Extractor, InputType
from etk.etk_extraction import Extraction, Extractable


class HTMLMetadataExtractor(Extractor):
    """
    Extracts META, microdata, JSON-LD and RDFa from HTML pages.

    Uses https://stackoverflow.com/questions/36768068/get-meta-tag-content-property-with-beautifulsoup-and-python to
    extract the META tags

    Uses https://github.com/scrapinghub/extruct to extract metadata from HTML pages
    """

    def __init__(self):
        Extractor.__init__(self,
                           input_type=InputType.HTML,
                           category="HTML extractor",
                           name="HTML metadata extractor")

    def extract(self, html_text: str,
                extract_title: bool = False,
                extract_meta: bool = False,
                extract_microdata: bool = False,
                extract_json_ld: bool = False,
                extract_rdfa: bool = False) \
            -> List[dict]:
        """

        Args:
            html_text ():
            extract_title (): extract the <title> tag from the HTML page, return as { "title": "..." }
            extract_meta (): extract the meta tags, return as { "meta": { "author": "...", ...}}
            extract_microdata (): extract microdata, returns as { "microdata": [...] }
            extract_json_ld (): extract JSON-LD, return as { "json-ld": [...] }
            extract_rdfa (): extract rdfa, returns as { "rdfa": [...] }

        Returns: a singleton list containing a dict with each type of metadata.

        """
        pass
