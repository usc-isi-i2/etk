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
        self.assertTrue('value' in result[0])
        self.assertTrue('context' in result[0])
        self.assertEqual(result[0]['context']['start'],
                         33)
        self.assertEqual(result[0]['context']['end'],
                         60)
        self.assertEqual(result[0]['value'],
                         'brun1989.itldc-customer.net')
        self.assertEqual(result[1]['value'],
                         'www.google.com')
        #Check if it does extract any illegal hostname
        str = "Illegal hostname.....a ..www..google..com."
        result = hostname_extractor.extract_hostname(str)
        self.assertEqual(len(result),0)
if __name__ == '__main__':
    unittest.main()