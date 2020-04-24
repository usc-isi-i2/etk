# import unittest
# from etk.extractors.spacy_rule_extractor import SpacyRuleExtractor
# import spacy
# from etk.extractors.html_metadata_extractor import HTMLMetadataExtractor
# import json
#
#
# class TestSpacyRuleExtractor(unittest.TestCase):
#
#     def test_SpacyRuleExtractor(self) -> None:
#         hme = HTMLMetadataExtractor()
#         with open('etk/unit_tests/ground_truth/news.html', 'r') as f:
#             sample_html = f.read()
#
#         sample_rules = json.load(open('etk/unit_tests/ground_truth/sample_spacy_rule.json'))
#
#         title_extraction = hme.extract(sample_html, extract_title=True)[0].value
#
#         sample_rule_extractor = SpacyRuleExtractor(spacy.load("en_core_web_sm"), sample_rules, "dummy")
#         extractions = sample_rule_extractor.extract(title_extraction)
#         expected_extraction = 'Trump'
#         self.assertEqual(extractions[0].value, expected_extraction)
#
#
# if __name__ == '__main__':
#     unittest.main()
