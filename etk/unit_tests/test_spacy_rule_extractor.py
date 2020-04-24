import unittest
from etk.extractors.spacy_rule_extractor import SpacyRuleExtractor
import spacy
import json

rules = json.load(open('etk/unit_tests/ground_truth/spacy_rules.json', "r"))


class TestSpacyRuleExtractor(unittest.TestCase):

    def setUp(self):
        self.nlp = spacy.load("en_core_web_sm")

    def test_SpacyRuleExtractor_word_1(self) -> None:
        sample_rules = rules["test_SpacyRuleExtractor_word_1"]
        sample_rule_extractor = SpacyRuleExtractor(self.nlp, sample_rules, "test_extractor")
        extractions = sample_rule_extractor.extract(
            "version 2 of etk, implemented by Runqi Shao, Dongyu Li, Sylvia lin, Amandeep and others.")

        expected = [('rule_0', 'Name: Runqi Shao'), ('rule_0', 'Name: Dongyu Li'), ('rule_0', 'Name: Sylvia'),
                    ('rule_0', 'Name: Amandeep')]
        self.assertEqual([(x.rule_id, x.value) for x in extractions], expected)

    def test_SpacyRuleExtractor_word_2(self) -> None:
        sample_rules = rules["test_SpacyRuleExtractor_word_2"]
        sample_rule_extractor = SpacyRuleExtractor(self.nlp, sample_rules, "test_extractor")
        extractions = sample_rule_extractor.extract(
            "version 2 of etk, implemented by Runqi Shao, Dongyu Li, Sylvia lin, Amandeep and others.")
        expected = [('rule_0', 'Name: Sylvia lin')]
        self.assertEqual([(x.rule_id, x.value) for x in extractions], expected)

    def test_SpacyRuleExtractor_word_3(self) -> None:
        sample_rules = rules["test_SpacyRuleExtractor_word_3"]

        sample_rule_extractor = SpacyRuleExtractor(self.nlp, sample_rules, "test_extractor")
        extractions = sample_rule_extractor.extract(
            "version 2 of etk, implemented by Runqi Shao, DongYu94 Li, Sylvia lin, Amandeep and others.")

        expected = [('rule_1', 'Name: Runqi Shao'), ('rule_0', 'Name: DongYu94 Li')]
        self.assertEqual([(x.rule_id, x.value) for x in extractions], expected)

    def test_SpacyRuleExtractor_word_4(self) -> None:
        sample_rules = rules["test_SpacyRuleExtractor_word_4"]

        sample_rule_extractor = SpacyRuleExtractor(self.nlp, sample_rules, "test_extractor")
        extractions = sample_rule_extractor.extract(
            "version 2 of etk, implemented by Runqi Shao, DongYu94 Li, Sylvia lin, Amandeep and others.")

        expected = [('rule_0', 'Name: Sylvia')]
        self.assertEqual([(x.rule_id, x.value) for x in extractions], expected)

    def test_SpacyRuleExtractor_word_5(self) -> None:
        sample_rules = rules["test_SpacyRuleExtractor_word_5"]

        sample_rule_extractor = SpacyRuleExtractor(self.nlp, sample_rules, "test_extractor")
        extractions = sample_rule_extractor.extract(
            "version 2 of etk, implemented by Runqi Shao, DongYu94 Li, Sylvia lin, Amandeep and others.")
        expected = [('rule_0', 'First Name: Runqi, Last Name: Shao. Full name: Runqi Shao'),
                    ('rule_0', 'First Name: DongYu94, Last Name: Li. Full name: DongYu94 Li')]
        self.assertEqual([(x.rule_id, x.value) for x in extractions], expected)

    def test_SpacyRuleExtractor_number_1(self) -> None:
        sample_rules = rules["test_SpacyRuleExtractor_number_1"]

        sample_rule_extractor = SpacyRuleExtractor(self.nlp, sample_rules, "test_extractor")
        extractions = sample_rule_extractor.extract(
            "Extract from the following number: 32 12 54435 23 665.3 34 65.42 23 4545")

        expected = [('rule_0', '665.3'), ('rule_0', '4545')]
        self.assertEqual([(x.rule_id, x.value) for x in extractions], expected)

    def test_SpacyRuleExtractor_number_2(self) -> None:
        sample_rules = rules["test_SpacyRuleExtractor_number_2"]

        sample_rule_extractor = SpacyRuleExtractor(self.nlp, sample_rules, "test_extractor")
        extractions = sample_rule_extractor.extract(
            "Extract from the following number: 32 12 54435 23 665.3 34 65.42 23 4545")

        expected = [('rule_0', '32'), ('rule_0', '12'), ('rule_0', '23'), ('rule_0', '34'), ('rule_0', '23'), ('rule_0', '4545')]
        self.assertEqual([(x.rule_id, x.value) for x in extractions], expected)

    def test_SpacyRuleExtractor_shape_1(self) -> None:
        sample_rules = rules["test_SpacyRuleExtractor_shape_1"]

        sample_rule_extractor = SpacyRuleExtractor(self.nlp, sample_rules, "test_extractor")
        extractions = sample_rule_extractor.extract(
            "version 2 of etk, implemented by RqS, DongYu94 Li, Sylvia lin, Amandeep and others.")

        expected = [('rule_0', 'RqS'), ('rule_0', 'DongYu94')]
        self.assertEqual([(x.rule_id, x.value) for x in extractions], expected)

    def test_SpacyRuleExtractor_punc_1(self) -> None:
        sample_rules = rules["test_SpacyRuleExtractor_punc_1"]

        sample_rule_extractor = SpacyRuleExtractor(self.nlp, sample_rules, "test_extractor")
        extractions = sample_rule_extractor.extract(
            "version 2 of etk, implemented by Rq? Shao. DongYu94 Li, Sylvia-lin, Amandeep and others.")

        expected = [('rule_0', 'Name: Rq, Shao'), ('rule_0', 'Name: Sylvia, lin')]
        self.assertEqual([(x.rule_id, x.value) for x in extractions], expected)

    def test_SpacyRuleExtractor_linebreak_1(self) -> None:
        sample_rules = rules["test_SpacyRuleExtractor_linebreak_1"]

        sample_rule_extractor = SpacyRuleExtractor(self.nlp, sample_rules, "test_extractor")
        extractions = sample_rule_extractor.extract(
            "version 2 of etk, implemented by Rq? Shao. DongYu94 Li, \n\n\n Sylvia-lin, Amandeep and others.")

        expected = [('rule_0', 'Length 3 linebreak: \n\n\n ')]
        self.assertEqual([(x.rule_id, x.value) for x in extractions], expected)

    def test_SpacyRuleExtractor_output_format_1(self) -> None:
        sample_rules = rules["test_SpacyRuleExtractor_output_format_1"]

        sample_rule_extractor = SpacyRuleExtractor(self.nlp, sample_rules, "test_extractor")
        extractions = sample_rule_extractor.extract(
            "version 2 of etk, implemented by Runqi Shao, Dongyu Li, Sylvia lin, Amandeep and others.")

        expected = [('rule_0', 'Name: Sylvia, lin')]
        self.assertEqual([(x.rule_id, x.value) for x in extractions], expected)

    def test_SpacyRuleExtractor_output_format_2(self) -> None:
        sample_rules = rules["test_SpacyRuleExtractor_output_format_2"]

        sample_rule_extractor = SpacyRuleExtractor(self.nlp, sample_rules, "test_extractor")
        extractions = sample_rule_extractor.extract(
            "version 2 of etk, implemented by Runqi Shao, Dongyu Li, Sylvia lin, Amandeep and others.")

        expected = [('rule_0', 'Name: Sylvia, lin')]
        self.assertEqual([(x.rule_id, x.value) for x in extractions], expected)

    def test_SpacyRuleExtractor_output_format_3(self) -> None:
        sample_rules = rules["test_SpacyRuleExtractor_output_format_3"]

        sample_rule_extractor = SpacyRuleExtractor(self.nlp, sample_rules, "test_extractor")
        extractions = sample_rule_extractor.extract(
            "version 2 of etk, implemented by Runqi Shao, Dongyu Li, Sylvia lin, Amandeep and others.")

        expected = [('rule_0', 'Name: Sylvia, , lin')]
        self.assertEqual([(x.rule_id, x.value) for x in extractions], expected)


if __name__ == '__main__':
    unittest.main()
