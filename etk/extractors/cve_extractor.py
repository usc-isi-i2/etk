from etk.extractors.regex_extractor import RegexExtractor


class CVEExtractor(RegexExtractor):
    def __init__(self):
        cve_pattern = r"CVE-(?:\d{4})-(?:\d{4})"
        RegexExtractor.__init__(self, pattern=cve_pattern, extractor_name="cve extractor")
