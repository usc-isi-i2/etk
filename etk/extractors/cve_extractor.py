import re

from etk.extractors.regex_extractor import RegexExtractor


class CVEExtractor(RegexExtractor):
    """
    **Description**
        This class inherits RegexExtractor by predefine the CVE regex pattern
        CVE id form: https://cve.mitre.org/about/faqs.html#what_is_cve_id

    Examples:
        ::

            CVE_extractor = CVEExtractor()
            CVE_extractor.extract(text=input_doc)

    """
    def __init__(self):
        cve_pattern = r"CVE-(?:\d{4})-(?:\d{4,7})"
        RegexExtractor.__init__(self, pattern=cve_pattern, flags=re.IGNORECASE,
                                extractor_name="cve extractor")
