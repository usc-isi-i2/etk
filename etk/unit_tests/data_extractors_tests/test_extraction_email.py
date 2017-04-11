import unittest
import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from data_extractors.digEmailExtractor import email_extractor


class TestEmailExtractorMethods(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_email_extractor_include_context(self):
        print "data_extractors.email.email_extractor"
        text = 'To Make A Booking You Can Call : +31 6 57 921 210 Between 10AM and 1AM Or Email ' \
               'misstheresaberkley@gmail.com For More Information Please Visit http://www.theresaberkley.com'

        include_context = True
        expected_extraction = [
            {'context': {'start': 80, 'obfuscation': False, 'end': 108}, 'value': 'misstheresaberkley@gmail.com'}]
        extraction = email_extractor.extract(text, include_context)
        self.assertEqual(extraction, expected_extraction)

    def test_email_extractor_not_include_context(self):
        print "data_extractors.email.email_extractor_no_context"
        text = 'To Make A Booking You Can Call : +31 6 57 921 210 Between 10AM and 1AM Or Email ' \
               'misstheresaberkley@gmail.com For More Information Please Visit http://www.theresaberkley.com'

        include_context = False
        expected_extraction = ['misstheresaberkley@gmail.com']
        extraction = email_extractor.extract(text, include_context)
        self.assertEqual(extraction, expected_extraction)


if __name__ == '__main__':
    unittest.main()