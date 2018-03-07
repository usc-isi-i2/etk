import re
from etk.extractors.TEMP_simple_regex_extractor import SimpleRegexExtractor


class HostnameExtractor(SimpleRegexExtractor):
    def __init__(self):
        hostname_pattern = re.compile(r"\b(?:[a-zA-Z0-9](?:[a-zA-Z0-9\-]{,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,6}\b")
        SimpleRegexExtractor.__init__(self,
                                      pattern=hostname_pattern,
                                      name="hostname extractor")
