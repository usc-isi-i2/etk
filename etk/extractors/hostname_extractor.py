from etk.extractors.regex_extractor import RegexExtractor


class HostnameExtractor(RegexExtractor):
    def __init__(self):
        hostname_pattern = r"\b(?:[a-zA-Z0-9](?:[a-zA-Z0-9\-]{,61}[a-zA-Z0-9])?\.)+" \
                           r"(?!html|php|jsp|xml|pdf|asp|css|aspx|phtml)[a-zA-Z]{2,6}\b"
        RegexExtractor.__init__(self, pattern=hostname_pattern, extractor_name="hostname extractor")
