import unittest
from etk.extractors.spacy_ner_extractor import SpacyNerExtractor
from etk.black_list_filter import BlackListFilter
from etk.extractors.glossary_extractor import GlossaryExtractor
from etk.tokenizer import Tokenizer


class TestBlackListFilter(unittest.TestCase):
    def setUp(self):
        self.text = 'Napoléon Bonaparte was a French statesman and military leader who rose to prominence during the ' \
                    'French Revolution and led several successful campaigns during the French Revolutionary Wars. ' \
                    'As Napoleon, he was Emperor of the French from 1804 until 1814, and again briefly in 1815 during ' \
                    'the Hundred Days. Napoleon dominated European and global affairs for more than a decade while ' \
                    'leading France against a series of coalitions in the Napoleonic Wars. He won most of these wars ' \
                    'and the vast majority of his battles, building a large empire that ruled over continental Europe ' \
                    'before its final collapse in 1815. He is considered one of the greatest commanders in history, ' \
                    'and his wars and campaigns are studied at military schools worldwide. Napoleon\'s political and ' \
                    'cultural legacy has endured as one of the most celebrated and controversial leaders in human history.'
        extractor = SpacyNerExtractor(extractor_name='spacy_ner_extractor')
        self.results = extractor.extract(self.text)

        glossary_1 = ['Beijing', 'Los Angeles', 'New York', 'Shanghai']
        t = Tokenizer()
        text = 'i live in los angeles. my hometown is Beijing. I love New York City.'
        tokens = t.tokenize(text)
        ge = GlossaryExtractor(glossary_1, 'test_glossary', t, 3, False)
        self.results2 = ge.extract(tokens)

    def test_black_list_filter(self) -> None:
        black_list = ["Napoléon Bonaparte", "France", "Napoleon"]
        blf = BlackListFilter(black_list)
        filtered_extractions = blf.filter(self.results)

        self.assertTrue(len(filtered_extractions) == 0)

    def test_black_list_filter_case_sensitive(self) -> None:
        black_list = ["Napoléon Bonaparte", "France", "napoleon"]
        blf = BlackListFilter(black_list)
        filtered_extractions = blf.filter(self.results, case_sensitive=True)
        self.assertTrue(len(filtered_extractions) == 2)

    def test_glossary_filter(self):
        black_list = ["los angeles"]
        blf = BlackListFilter(black_list)
        filtered_extractions = blf.filter(self.results2)
        self.assertTrue(len(filtered_extractions) == 2)


if __name__ == '__main__':
    unittest.main()
