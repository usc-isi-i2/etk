import unittest
from etk.extractors.bitcoin_address_extractor import BitcoinAddressExtractor
from etk.extractors.cryptographic_hash_extractor import CryptographicHashExtractor
from etk.extractors.cve_extractor import CVEExtractor
from etk.extractors.hostname_extractor import HostnameExtractor
from etk.extractors.ip_address_extractor import IPAddressExtractor
from etk.extractors.url_extractor import URLExtractor


class TestMultiDataExtractor(unittest.TestCase):

    def test_bitcoin_address_extractor(self) -> None:
        text = "1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2 3J98t1WpEZ73CNmQviecrnyiWrnqRhWNLy " \
               "bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq bc1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2 " \
               "1AGNa15ZQXAZUgFiqJ3i7Z2DPU2J6hW62i 17NdbrSGoUotzeGCcMMCqnFkEvLymoou9j " \
               "bc1qc7slrfxkknqcq2jevvvkdgvrt8080852dfjewde450xdlk4ugp7szw5tk9 " \
               "bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq"
        e = BitcoinAddressExtractor()

        results = [x.value for x in e.extract(text)]
        expected = [
            "1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2",
            "3J98t1WpEZ73CNmQviecrnyiWrnqRhWNLy",
            "1AGNa15ZQXAZUgFiqJ3i7Z2DPU2J6hW62i",
            "17NdbrSGoUotzeGCcMMCqnFkEvLymoou9j"
        ]

        self.assertEqual(results, expected)

    def test_cryptographic_hash_extractor(self) -> None:
        text = "Sample 9e107d9d372bb6826bd81d3542a419d6 e4d909c290d0fb1ca068ffaddf22cbd0 " \
               "SHA1(The quick brown fox jumps over the lazy dog)\ngives hexadecimal: " \
               "2fd4e1c67a2d28fced849ee1bb76e7391b93eb12\ngives Base64 binary to ASCII text encoding: " \
               "L9ThxnotKPzthJ7hu3bnORuT6xI="
        e = CryptographicHashExtractor()

        results = [x.value for x in e.extract(text)]
        expected = [
            "9e107d9d372bb6826bd81d3542a419d6",
            "e4d909c290d0fb1ca068ffaddf22cbd0",
            "2fd4e1c67a2d28fced849ee1bb76e7391b93eb12"
        ]

        self.assertEqual(results, expected)

    def test_cve_extractor(self) -> None:
        text = "Sample cves are CVE-1993-1344,  CVE-2016-7654321, CVE-2006-1232 and CVE-1993-1344."
        e = CVEExtractor()

        results = [x.value for x in e.extract(text)]
        expected = [
            "CVE-1993-1344",
            "CVE-2016-7654321",
            "CVE-2006-1232",
            "CVE-1993-1344"
        ]

        self.assertEqual(results, expected)

    def test_hostname_extractor(self) -> None:
        text = "check sites brun1989.itldc-customer.net, www.google.com and " \
               "https://www.google.com go shopping with amazon.com"
        e = HostnameExtractor()

        results = [x.value for x in e.extract(text)]
        expected = [
            "brun1989.itldc-customer.net",
            "www.google.com",
            "www.google.com",
            "amazon.com"
        ]

        self.assertEqual(results, expected)

    def test_ip_address_extractor(self) -> None:
        text = "my ip address is 192.168.0.1 , 193.14.1.1 and 193.14.1.1 not 193.14"
        e = IPAddressExtractor()

        results = [x.value for x in e.extract(text)]
        expected = [
            "192.168.0.1",
            "193.14.1.1",
            "193.14.1.1"
        ]

        self.assertEqual(results, expected)

    def test_url_extractor(self) -> None:
        text = "https://foo_bar.example.com/ http://foo_bar.example.com/ https://foobar.example.com/xxx.php  " \
               "%%%###http://isi.edu/abx  dig.isi.edu/index.html"
        e = URLExtractor(True)

        results = [x.value for x in e.extract(text)]
        expected = [
            "https://foo_bar.example.com/",
            "http://foo_bar.example.com/",
            "https://foobar.example.com/xxx.php",
            "http://isi.edu/abx",
            "dig.isi.edu/index.html"
        ]

        self.assertEqual(results, expected)


if __name__ == '__main__':
    unittest.main()