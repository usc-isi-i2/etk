from typing import List
from etk.extractor import Extractor, InputType
from etk.etk_extraction import Extraction
import re

# A temp regex extractor,
# will be removed when the general regex extractor is implemented.
# Temporarily used for
#     bitcoin_address_extractor,
#     cryptographic_hash_extractor,
#     cve_extractor,
#     hostname_extractor,
#     ip_address_extractor,
#     url_extractor,


class SimpleRegexExtractor(Extractor):

    def __init__(self, pattern: re.compile or List[dict], name: str):

        """
        Args:
             pattern (): regex patten to extract data
        """
        self._pattern = pattern
        Extractor.__init__(self,
                           input_type=InputType.TEXT,
                           category="Regex Extractor",
                           name=name)

    @property
    def pattern(self):
        return self._pattern

    def extract(self, text: str) -> List[Extraction]:
        """
        Extracts text from an text using a regex

        Args:
             text (): text to be extracted

        Returns: a list of Extraction(s)
        """

        try:
            res = list()
            if text:
                if type(self.pattern) is list:
                    for p in self.pattern:
                        tag = p['tag']
                        iterator = p['pattern'].finditer(text)
                        if iterator:
                            for ele in iterator:
                                res.append(Extraction(ele.group(), extractor_name=self.name,
                                                      start_char=ele.span()[0], end_char=ele.span()[1], tag=tag))
                else:
                    iterator = self.pattern.finditer(text)
                    if iterator:
                        for ele in iterator:
                            res.append(Extraction(ele.group(), extractor_name=self.name,
                                                  start_char=ele.span()[0], end_char=ele.span()[1]))
            return res
        except Exception as e:
            print('Error in extracting through regex %s' % e)
            return list()




