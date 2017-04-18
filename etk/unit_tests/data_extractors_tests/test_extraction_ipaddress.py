import unittest

import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from data_extractors import ipaddress_extractor

class TestIpAddressExtractorMethods(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_ipaddress_extractor(self):
        doc = 'my ip address is 192.168.0.1 , 193.14.1.1 and 193.14.1.1 not 193.14'

        result = ipaddress_extractor.extract_ipaddress(doc)
        self.assertEqual(len(result),2)
        self.assertEqual(result[0],
                         '192.168.0.1')
        self.assertEqual(result[1],
                         '193.14.1.1')

if __name__ == '__main__':
    unittest.main()
