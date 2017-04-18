import unittest

import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))


from data_extractors import hostname_extractor

class TestHostnameExtractorMethods(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_hostname_extractor(self):
        str = 'Hi im Natsume, check out my site brun1989.itldc-customer.net, www.google.com and https://www.google.com'
        result = hostname_extractor.extract_hostname(str)
        self.assertEqual(len(result),2)
        self.assertEqual(result[0],
                         'brun1989.itldc-customer.net')
        self.assertEqual(result[1],
                         'www.google.com')

if __name__ == '__main__':
    unittest.main()