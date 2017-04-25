import unittest

import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from data_extractors import cve_extractor

class TestCveExtractorMethods(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_cve_extractor(self):
        str = 'Sample cves are CVE-1993-1344,  CVE-2006-1232 and CVE-1993-1344'

        extracted_cve    = cve_extractor.extract_cve(str)
        self.assertEqual(len(extracted_cve),2)
        self.assertTrue('value' in extracted_cve[0])
        self.assertTrue('context' in extracted_cve[0])
        self.assertEqual(extracted_cve[0]['context']['start'],
                         16)
        self.assertEqual(extracted_cve[0]['context']['end'],
                         29)
        self.assertEqual(extracted_cve[0]['value'],
                         'CVE-1993-1344')
        self.assertEqual(extracted_cve[1]['value'],
                         'CVE-2006-1232')

        str = "Sample cves are none"
        extracted_cve    = cve_extractor.extract_cve(str)
        self.assertEqual(len(extracted_cve),0)

        str= ""
        extracted_cve    = cve_extractor.extract_cve(str)
        self.assertEqual(len(extracted_cve),0)

if __name__ == '__main__':
    unittest.main()
