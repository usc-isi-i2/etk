import re
from etk.extractors.TEMP_simple_regex_extractor import SimpleRegexExtractor


class CryptographicHashExtractor(SimpleRegexExtractor):
    # TODO: It will be better if can record the type of the hash value
    def __init__(self):
        md5 = r"\b[a-fA-F\d]{32}\b"
        sha1 = r"\b[0-9a-f]{5,40}\b"
        sha256 = r"\b[A-Fa-f0-9]{64}\b"
        hash_pattern = re.compile(md5+"|"+sha1+"|"+sha256)
        SimpleRegexExtractor.__init__(self,
                                      pattern=hash_pattern,
                                      name="cryptographic hash extractor")

