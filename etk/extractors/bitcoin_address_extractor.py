import re
from etk.extractors.TEMP_simple_regex_extractor import SimpleRegexExtractor


class BitcpinAddressExtractor(SimpleRegexExtractor):
    def __init__(self, support_Bech32: bool=False):
        if support_Bech32:
            # a regex support Bech32 type (which is not supported for most applications)
            bitcoin_address_pattern = re.compile(r"\b[13][a-km-zA-HJ-NP-Z1-9]{25,34}|bc1[a-zA-HJ-NP-Z0-9]{39}|bc1[a-zA-HJ-NP-Z0-9]{59}\b")
        else:
            # simple version supporting P2PKH and P2SH
            bitcoin_address_pattern = re.compile(r"\b[13][a-km-zA-HJ-NP-Z1-9]{25,34}\b")
        SimpleRegexExtractor.__init__(self,
                                      pattern=bitcoin_address_pattern,
                                      name="bitcoin address extractor")
