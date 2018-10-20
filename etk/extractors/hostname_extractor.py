from etk.extractors.regex_extractor import RegexExtractor


class HostnameExtractor(RegexExtractor):
    """
    **Description**
        This class inherits RegexExtractor by predefining the regex pattern for hostname

    Examples:
        ::

            hostname_extractor = HostnameExtractor()
            hostname_extractor.extract(text=input_doc)

    """
    def __init__(self):
        hostname_pattern = r"\b(?:[a-zA-Z0-9](?:[a-zA-Z0-9\-]{,61}[a-zA-Z0-9])?\.)+" \
                           r"(?!html|php|jsp|xml|pdf|asp|css|aspx|phtml)[a-zA-Z]{2,6}\b"
        RegexExtractor.__init__(self, pattern=hostname_pattern, extractor_name="hostname extractor")
