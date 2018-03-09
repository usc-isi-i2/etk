from etk.extractors.regex_extractor import RegexExtractor


class HostnameExtractor(RegexExtractor):
    # TODO: will extract things like "index.html", "about.php", should be improved
    def __init__(self):
        hostname_pattern = r"\b(?:[a-zA-Z0-9](?:[a-zA-Z0-9\-]{,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,6}\b"
        RegexExtractor.__init__(self, pattern=hostname_pattern, extractor_name="hostname extractor")
