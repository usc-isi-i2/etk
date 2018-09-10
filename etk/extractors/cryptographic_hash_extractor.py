from etk.extractors.regex_extractor import RegexExtractor
from etk.extractor import Extractor, InputType
from typing import List
from etk.extraction import Extraction


class CryptographicHashExtractor(Extractor):
    """
    **Description**
        This class inherits RegexExtractor by predefine the regex patterns for md5, sha1 and sha256

    Examples:
        ::

            cryptographic_hash_extractor = CryptographicHashExtractor()
            cryptographic_hash_extractor.extract(text=input_doc)

    """
    def __init__(self):
        e_name = 'cryptographic hash extractor'
        self._regex_extractors = [
            RegexExtractor(r"(\b[a-fA-F\d]{32}\b)", 'md5 '+e_name, general_tag='md5'),
            RegexExtractor(r"(\b[0-9a-f]{40}\b)", 'sha1 '+e_name, general_tag='sha1'),
            RegexExtractor(r"(\b[A-Fa-f0-9]{64}\b)", 'sha256 '+e_name, general_tag='sha256'),
        ]
        Extractor.__init__(self,
                           input_type=InputType.TEXT,
                           category="regex",
                           name=e_name)

    @property
    def regex_extractors(self):
        return self._regex_extractors

    def extract(self, text: str) -> List[Extraction]:
        """

        Args:
            text (str): The input source to be processed

        Returns:
            List[Extraction]: The list of extractions returned by CryptographicHashExtractor

        """

        res = list()
        for e in self.regex_extractors:
            res = res+e.extract(text)
        return res
