import re
from etk.extractors.TEMP_simple_regex_extractor import SimpleRegexExtractor


class CVEExtractor(SimpleRegexExtractor):
    def __init__(self):
        cve_pattern = re.compile(r"CVE-(\d{4})-(\d{4})")
        SimpleRegexExtractor.__init__(self,
                                      pattern=cve_pattern,
                                      name="cve extractor")
