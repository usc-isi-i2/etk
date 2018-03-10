import unittest
from etk.extractors.regex_extractor import RegexExtractor, MatchMode


class TestRegexExtractor(unittest.TestCase):
    def test_match_mode_with_group(self) -> None:
        regexp = RegexExtractor('(.)@(.)', 'test_extractor')
        test_str = 'a@1, b@2, c@3, d@4'

        extractions_with_flag_0 = regexp.extract(test_str, 0, MatchMode.MATCH)
        extractions_with_flag_5 = regexp.extract(test_str, 5, MatchMode.MATCH)

        res_with_flag_0 = [ex.value for ex in extractions_with_flag_0]
        res_with_flag_5 = [ex.value for ex in extractions_with_flag_5]

        expected_res_with_flag_0 = ['a', '1']
        expected_res_with_flag_5 = ['b', '2']
        self.assertEqual(res_with_flag_0, expected_res_with_flag_0)
        self.assertEqual(res_with_flag_5, expected_res_with_flag_5)

    def test_search_mode_with_group(self) -> None:
        regexp = RegexExtractor('(.)@(.)', 'test_extractor')
        test_str = 'testtesttest, a@1, b@2, c@3, d@4'

        extractions_with_flag_0 = regexp.extract(test_str, 0, MatchMode.SEARCH)
        extractions_with_flag_18 = regexp.extract(test_str, 18, MatchMode.SEARCH)

        res_with_flag_0 = [ex.value for ex in extractions_with_flag_0]
        res_with_flag_18 = [ex.value for ex in extractions_with_flag_18]

        expected_res_with_flag_0 = ['a', '1']
        expected_res_with_flag_18 = ['b', '2']
        self.assertEqual(res_with_flag_0, expected_res_with_flag_0)
        self.assertEqual(res_with_flag_18, expected_res_with_flag_18)

    def test_match_mode_without_group(self) -> None:
        regexp = RegexExtractor('.@.', 'test_extractor')
        test_str = 'a@1, b@2, c@3, d@4'

        extractions_with_flag_0 = regexp.extract(test_str, 0, MatchMode.MATCH)
        extractions_with_flag_5 = regexp.extract(test_str, 5, MatchMode.MATCH)

        res_with_flag_0 = [ex.value for ex in extractions_with_flag_0]
        res_with_flag_5 = [ex.value for ex in extractions_with_flag_5]

        expected_res_with_flag_0 = ['a@1']
        expected_res_with_flag_5 = ['b@2']
        self.assertEqual(res_with_flag_0, expected_res_with_flag_0)
        self.assertEqual(res_with_flag_5, expected_res_with_flag_5)

    def test_search_mode_without_group(self) -> None:
        regexp = RegexExtractor('.@.', 'test_extractor')
        test_str = 'testtesttest, a@1, b@2, c@3, d@4'

        extractions_with_flag_0 = regexp.extract(test_str, 0, MatchMode.SEARCH)
        extractions_with_flag_18 = regexp.extract(test_str, 18, MatchMode.SEARCH)

        res_with_flag_0 = [ex.value for ex in extractions_with_flag_0]
        res_with_flag_18 = [ex.value for ex in extractions_with_flag_18]

        expected_res_with_flag_0 = ['a@1']
        expected_res_with_flag_18 = ['b@2']
        self.assertEqual(res_with_flag_0, expected_res_with_flag_0)
        self.assertEqual(res_with_flag_18, expected_res_with_flag_18)

    def test_findall_mode_with_group(self) -> None:
        regexp = RegexExtractor('(.)@(.)', 'test_extractor')
        test_str = 'a@1, b@2, c@3, d@4'

        extractions_with_flag_0 = regexp.extract(test_str, 0, MatchMode.FINDALL)
        extractions_with_flag_5 = regexp.extract(test_str, 5, MatchMode.FINDALL)

        res_with_flag_0 = [ex.value for ex in extractions_with_flag_0]
        res_with_flag_5 = [ex.value for ex in extractions_with_flag_5]

        expected_res_with_flag_0 = ['a', '1', 'b', '2', 'c', '3', 'd', '4']
        expected_res_with_flag_5 = ['b', '2', 'c', '3', 'd', '4']

        self.assertEqual(res_with_flag_0, expected_res_with_flag_0)
        self.assertEqual(res_with_flag_5, expected_res_with_flag_5)

    def test_findall_mode_without_group(self) -> None:
        regexp = RegexExtractor('.@.', 'test_extractor')
        test_str = 'a@1, b@2, c@3, d@4'

        extractions_with_flag_0 = regexp.extract(test_str, 0, MatchMode.FINDALL)
        extractions_with_flag_5 = regexp.extract(test_str, 5, MatchMode.FINDALL)

        res_with_flag_0 = [ex.value for ex in extractions_with_flag_0]
        res_with_flag_5 = [ex.value for ex in extractions_with_flag_5]

        expected_res_with_flag_0 = ['a@1', 'b@2', 'c@3', 'd@4']
        expected_res_with_flag_5 = ['b@2', 'c@3', 'd@4']

        self.assertEqual(res_with_flag_0, expected_res_with_flag_0)
        self.assertEqual(res_with_flag_5, expected_res_with_flag_5)

    def test_split_mode(self) -> None:
        regexp = RegexExtractor(',', 'test_extractor')
        test_str = 'a@1, b@2, c@3, d@4'

        extractions_with_flag_0 = regexp.extract(test_str, 0, MatchMode.SPLIT)
        extractions_with_flag_2 = regexp.extract(test_str, 2, MatchMode.SPLIT)

        res_with_flag_0 = [ex.value for ex in extractions_with_flag_0]
        res_with_flag_2 = [ex.value for ex in extractions_with_flag_2]

        expected_res_with_flag_0 = ['a@1', ' b@2', ' c@3', ' d@4']
        expected_res_with_flag_2 = ['a@1', ' b@2', ' c@3, d@4']

        self.assertEqual(res_with_flag_0, expected_res_with_flag_0)
        self.assertEqual(res_with_flag_2, expected_res_with_flag_2)
