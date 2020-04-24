# from typing import List
# from etk.extractor import Extractor, InputType
# from etk.extraction import Extraction, Extractable
# from extruct.w3cmicrodata import MicrodataExtractor
# from extruct.jsonld import JsonLdExtractor
# from extruct.rdfa import RDFaExtractor
# from bs4 import BeautifulSoup
#
#
# class HTMLMetadataExtractor(Extractor):
#     """
#     **Description**
#         Extracts META, microdata, JSON-LD and RDFa from HTML pages.
#
#         Uses https://stackoverflow.com/questions/36768068/get-meta-tag-content-property-with-beautifulsoup-and-python to
#         extract the META tags
#
#         Uses https://github.com/scrapinghub/extruct to extract metadata from HTML pages
#
#     Examples:
#         ::
#
#             html_metadata_extractor = HTMLMetadataExtractor()
#             html_metadata_extractor.extract(text=input_doc,
#                                             extract_title=True,
#                                             extract_meta=True,
#                                             extract_microdata=False,
#                                             extract_json_ld=False,
#                                             extract_rdfa=False,
#                                             rdfa_base_url="")
#
#     """
#
#     def __init__(self):
#         Extractor.__init__(self,
#                            input_type=InputType.HTML,
#                            category="HTML extractor",
#                            name="HTML metadata extractor")
#
#     def extract(self, html_text: str,
#                 extract_title: bool = False,
#                 extract_meta: bool = False,
#                 extract_microdata: bool = False,
#                 microdata_base_url: str = "",
#                 extract_json_ld: bool = False,
#                 extract_rdfa: bool = False,
#                 rdfa_base_url: str = "") \
#             -> List[Extraction]:
#         """
#         Args:
#             html_text (str): input html string to be extracted
#             extract_title (bool): True if string of 'title' tag needs to be extracted, return as { "title": "..." }
#             extract_meta (bool): True if string of 'meta' tags needs to be extracted, return as { "meta": { "author": "...", ...}}
#             extract_microdata (bool): True if microdata needs to be extracted, returns as { "microdata": [...] }
#             microdata_base_url (str): base namespace url for microdata, empty string if no base url is specified
#             extract_json_ld (bool): True if json-ld needs to be extracted, return as { "json-ld": [...] }
#             extract_rdfa (bool): True if rdfs needs to be extracted, returns as { "rdfa": [...] }
#             rdfa_base_url (str): base namespace url for rdfa, empty string if no base url is specified
#
#         Returns:
#             List[Extraction]: the list of extraction or the empty list if there are no matches.
#         """
#         res = list()
#         soup = BeautifulSoup(html_text, 'html.parser')
#
#         if soup.title and extract_title:
#             title = self._wrap_data("title", soup.title.string.encode('utf-8').decode('utf-8'))
#             res.append(title)
#
#         if soup.title and extract_meta:
#             meta_content = self._wrap_meta_content(soup.find_all("meta"))
#             meta_data = self._wrap_data("meta", meta_content)
#             res.append(meta_data)
#
#         if extract_microdata:
#             mde = MicrodataExtractor()
#             mde_data = self._wrap_data("microdata", mde.extract(html_text, microdata_base_url))
#             res.append(mde_data)
#
#         if extract_json_ld:
#             jslde = JsonLdExtractor()
#             jslde_data = self._wrap_data("json-ld", jslde.extract(html_text))
#             res.append(jslde_data)
#
#         if extract_rdfa:
#             rdfae = RDFaExtractor()
#             rdfae_data = self._wrap_data("rdfa", rdfae.extract(html_text, rdfa_base_url))
#             res.append(rdfae_data)
#
#         return res
#
#     def _wrap_data(self, key: str, value) -> Extraction:
#         e = Extraction(value=value, extractor_name=self.name, tag=key)
#         return e
#
#     @staticmethod
#     def _wrap_meta_content(meta_tags) -> dict:
#         meta = {}
#         for tag in meta_tags:
#             meta[tag.get("name")] = tag.get("content")
#
#         return meta
