from typing import List
from etk.extractor import Extractor, InputType
from etk.extraction import Extraction, Extractable
from extruct.w3cmicrodata import MicrodataExtractor
from extruct.jsonld import JsonLdExtractor
from extruct.rdfa import RDFaExtractor
from bs4 import BeautifulSoup


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

    def extract(self, html_text: str,
                extract_title: bool = False,
                extract_meta: bool = False,
                extract_microdata: bool = False,
                extract_json_ld: bool = False,
                extract_rdfa: bool = False) \
            -> List[Extraction]:

        res = list()
        soup = BeautifulSoup(html_text, 'html.parser')

        if soup.title and extract_title:
            title = self.wrap_data("title", soup.title.string.encode('utf-8').decode('utf-8'))
            res.append(title)

        if soup.title and extract_meta:
            meta_content = self.wrap_meta_content(soup.find_all("meta"))
            meta_data = self.wrap_data("meta", meta_content)
            res.append(meta_data)

        if extract_microdata:
            mde = MicrodataExtractor()
            mde_data = self.wrap_data("microdata", mde.extract(html_text))
            res.append(mde_data)

        if extract_json_ld:
            jslde = JsonLdExtractor()
            jslde_data = self.wrap_data("json-ld", jslde.extract(html_text))
            res.append(jslde_data)

        if extract_rdfa:
            rdfae = RDFaExtractor()
            rdfae_data = self.wrap_data("rdfa", rdfae.extract(html_text))
            res.append(rdfae_data)

        return res

    def wrap_data(self, key: str, value) -> Extraction:
        e = Extraction(value=value, extractor_name=self.name, tag=key)
        return e

    @staticmethod
    def wrap_meta_content(meta_tags) -> dict:
        meta = {}
        for tag in meta_tags:
            meta[tag.get("name")] = tag.get("content")

        return meta
