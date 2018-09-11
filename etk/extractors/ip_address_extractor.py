from etk.extractors.regex_extractor import RegexExtractor


class IPAddressExtractor(RegexExtractor):
    """
    **Description**
           This class inherits RegexExtractor and predefines the ip address pattern

    Examples:
        ::

            ip_address_extractor = IPAddressExtractor()
            ip_address_extractor.extract(text=input_doc)
    """
    def __init__(self):
        ip_address_pattern = r"(?:(?:[01]?[0-9]?[0-9]|2[0-4][0-9]|25[0-5])" \
                             r"[ (?:\[]?(?:\.|dot)[ )\]]?){3}(?:[01]?[0-9]?[0-9]|2[0-4][0-9]|25[0-5])"
        RegexExtractor.__init__(self, pattern=ip_address_pattern, extractor_name="ip address extractor")
