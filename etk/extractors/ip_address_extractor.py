from etk.extractors.regex_extractor import RegexExtractor


class IPAddressExtractor(RegexExtractor):
    def __init__(self):
        ip_address_pattern = r"(?:(?:[01]?[0-9]?[0-9]|2[0-4][0-9]|25[0-5])" \
                             r"[ (?:\[]?(?:\.|dot)[ )\]]?){3}(?:[01]?[0-9]?[0-9]|2[0-4][0-9]|25[0-5])"
        RegexExtractor.__init__(self, pattern=ip_address_pattern, extractor_name="ip address extractor")
