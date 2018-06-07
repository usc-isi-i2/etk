import json, os, sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from etk.etk import ETK
from etk.extractors.bitcoin_address_extractor import BitcoinAddressExtractor
from etk.extractors.cryptographic_hash_extractor import CryptographicHashExtractor
from etk.extractors.cve_extractor import CVEExtractor
from etk.extractors.hostname_extractor import HostnameExtractor
from etk.extractors.ip_address_extractor import IPAddressExtractor
from etk.extractors.url_extractor import URLExtractor
from etk.etk_module import ETKModule


class RegexETKModule(ETKModule):
    """
    Abstract class for extraction module
    """

    def __init__(self, etk):
        ETKModule.__init__(self, etk)
        bae = BitcoinAddressExtractor()
        ce = CVEExtractor()
        che = CryptographicHashExtractor()
        he = HostnameExtractor()
        iae = IPAddressExtractor()
        ue = URLExtractor(True)
        self.e_list = [bae, ce, che, he, iae, ue]

    def process_document(self, doc):
        """
        Add your code for processing the document
        """

        segment = doc.select_segments("target_text")[0]

        for e in self.e_list:
            res = doc.extract(e, segment)
            doc.store(res, e.name)
        return list()


if __name__ == "__main__":
    sample_input = {
        "target_text": "This is for test multiple extractors using regex expression. "
                       "A module can be used for bitcoin address extraction is "
                       "https://github.com/nederhoed/python-bitcoinaddress, but import is always failed. "
                       "The auther said 'If you use this module, support me with bitcoins! "
                       "Any amount is appreciated. 1qYsJbtEWAeXMsbgxUgGsJsAp3VArsBRd'. "
                       "SHA1(The quick brown fox jumps over the lazy dog)\ngives hexadecimal: "
                       "2fd4e1c67a2d28fced849ee1bb76e7391b93eb12, SHA1('aaa')=7e240de74fb1ed08fa08d38063f6a6a91462a815,"
                       "and a MD5 sample can be e4d909c290d0fb1ca068ffaddf22cbd0. Some CVE can be CVE-1993-1344,  "
                       "CVE-2006-1232 and CVE-1993-1344. We search through "
                       "https://www.google.com and go shopping with amazon.com, sites brun1989.itldc-customer.net."
                       "my ip address is 192.168.0.1 , 193.14.1.1 and 193.14.1.1 not 193.14"
                       "https://foo_bar.example.com/   %%%###http://isi.edu/abx  dig.isi.edu/index.html"
    }

    etk = ETK(modules=RegexETKModule)
    doc = etk.create_document(sample_input)

    docs = etk.process_ems(doc)

    print(json.dumps(docs[0].value, indent=2))
