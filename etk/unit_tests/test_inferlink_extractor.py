import unittest, json
from etk.extractors.inferlink_extractor import InferlinkExtractor, InferlinkRuleSet


class TestInferlinkExtractor(unittest.TestCase):

    def test_inferlink_extractor(self) -> None:
        with open('etk/unit_tests/ground_truth/sample_html.jl', 'r') as f:
            sample_html = json.load(f)['raw_content']
        rules = InferlinkRuleSet(InferlinkRuleSet.load_rules_file('etk/unit_tests/ground_truth/sample_inferlink_rules.json'))

        e = InferlinkExtractor(rules)
        test_result = e.extract(sample_html)

        result = [x.value for x in test_result]

        expected = [
            "323-452-2013",
            "Los Angeles, California",
            "23",
            "Hey I'm luna 3234522013 Let's explore , embrace and indulge in your favorite fantasy % independent. "
            "discreet no drama Firm Thighs and Sexy. My Soft skin & Tight Grip is exactly what you deserve Call or "
            "text Fetish friendly Fantasy friendly Party friendly 140 Hr SPECIALS 3234522013",
            "2017-01-02 06:46"
        ]

        self.assertEqual(result, expected)


if __name__ == '__main__':
    unittest.main()