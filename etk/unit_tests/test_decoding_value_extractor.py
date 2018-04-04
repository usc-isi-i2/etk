import unittest
from etk.extractors.decoding_value_extractor import DecodingValueExtractor


class TestDictionaryExtractor(unittest.TestCase):
    def test_dictionary_extractor(self) -> None:
        decoding_dict = {
            'CA': 'California',
            'ny': 'New York',
            'AZ': ' Arizona',
            ' TX ': 'Texas',
            ' fl': 'Florida',
        }
        values = ['ca', 'CA', ' CA', ' ca', 'NY', ' ny', 'Az', 'AZ', 'az ', 'tx', 'tx ', 'TX', 'fl', 'FL', 'fl ']

        de_default = DecodingValueExtractor(decoding_dict, 'default_decoding') # strip_key and not case_sensitive
        de_case_sensitive = DecodingValueExtractor(decoding_dict, 'default_decoding', case_sensitive=True)
        de_not_strip_key = DecodingValueExtractor(decoding_dict, 'default_decoding', strip_key=False)
        de_strip_value = DecodingValueExtractor(decoding_dict, 'default_decoding', strip_value=True)

        results = list()
        results.append([de_default.extract(v)[0].value for v in values if de_default.extract(v)])
        results.append([de_case_sensitive.extract(v)[0].value for v in values if de_case_sensitive.extract(v)])
        results.append([de_not_strip_key.extract(v)[0].value for v in values if de_not_strip_key.extract(v)])
        results.append([de_strip_value.extract(v)[0].value for v in values if de_strip_value.extract(v)])

        expected = [
            [
                'California', 'California', 'California', 'California', 'New York', 'New York', ' Arizona',
                ' Arizona', ' Arizona', 'Texas', 'Texas', 'Texas', 'Florida', 'Florida', 'Florida'
            ],
            [
                'California', 'California', 'New York', ' Arizona', 'Texas', 'Florida', 'Florida'
            ],
            [
                'California', 'California', 'New York', ' Arizona', ' Arizona'
            ],
            [
                'California', 'California', 'California', 'California', 'New York', 'New York', 'Arizona',
                'Arizona', 'Arizona', 'Texas', 'Texas', 'Texas', 'Florida', 'Florida', 'Florida'
            ],
        ]

        self.assertEqual(results[:-1], expected[:-1])


if __name__ == '__main__':
    unittest.main()