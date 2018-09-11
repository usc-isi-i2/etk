from etk.extractors.regex_extractor import RegexExtractor


class BitcoinAddressExtractor(RegexExtractor):
    """
    **Description**
        This class inherits RegexExtractor by predefining the regex pattern based on conditions

    Examples:
        ::

            bitcoin_addr_extractor = BitcoinAddressExtractor(support_Bech32=True)
            bitcoin_addr_extractor.extract(text=input_doc)

    """
    def __init__(self, support_Bech32: bool=False):
        if support_Bech32:
            # a regex support Bech32 type (which is not supported for most applications)
            bitcoin_address_pattern = r"\b[13][a-km-zA-HJ-NP-Z1-9]{25,34}|bc1[a-zA-HJ-NP-Z0-9]{39}|bc1[a-zA-HJ-NP-Z0-9]{59}\b"
        else:
            # simple version supporting P2PKH and P2SH
            bitcoin_address_pattern = r"\b[13][a-km-zA-HJ-NP-Z1-9]{25,34}\b"
        RegexExtractor.__init__(self, pattern=bitcoin_address_pattern, extractor_name="bitcoin address extractor")
