from etk.extractors.regex_extractor import RegexExtractor


class CVEExtractor(RegexExtractor):
    """
    **Description**
        This class inherits RegexExtractor by predefine the CVE regex pattern

    Examples:
        ::

            CVE_extractor = CVEExtractor()
            CVE_extractor.extract(text=input_doc)

    """
    def __init__(self):
        cve_pattern = r"CVE-(?:\d{4})-(?:\d{4})"
        RegexExtractor.__init__(self, pattern=cve_pattern, extractor_name="cve extractor")
