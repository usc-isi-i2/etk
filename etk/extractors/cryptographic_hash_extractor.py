import re
from etk.extractors.TEMP_simple_regex_extractor import SimpleRegexExtractor


class CryptographicHashExtractor(SimpleRegexExtractor):
    # TODO: It will be better if can record the type of the hash value
    def __init__(self):
        md5 = re.compile(r"(\b[a-fA-F\d]{32}\b)")
        sha1 = re.compile(r"(\b[0-9a-f]{40}\b)")
        sha256 = re.compile(r"(\b[A-Fa-f0-9]{64}\b)")
        hash_pattern = [
            {'tag': 'md5', 'pattern': md5},
            {'tag': 'sha1', 'pattern': sha1},
            {'tag': 'sha256', 'pattern': sha256},
        ]
        SimpleRegexExtractor.__init__(self,
                                      pattern=hash_pattern,
                                      name="cryptographic hash extractor")
