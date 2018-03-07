import re
from etk.extractors.TEMP_simple_regex_extractor import SimpleRegexExtractor


class IPAddressExtractor(SimpleRegexExtractor):
    def __init__(self):
        ip_address_pattern = re.compile(r"((([01]?[0-9]?[0-9]|2[0-4][0-9]|25[0-5])[ (\[]?(\.|dot)[ )\]]?){3}([01]?[0-9]?[0-9]|2[0-4][0-9]|25[0-5]))")
        SimpleRegexExtractor.__init__(self,
                                      pattern=ip_address_pattern,
                                      name="ip address extractor")
