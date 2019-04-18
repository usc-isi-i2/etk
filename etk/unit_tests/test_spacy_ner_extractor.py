import unittest
from etk.extractors.spacy_ner_extractor import SpacyNerExtractor


class TestSpacyNerExtractor(unittest.TestCase):

    def test_spacy_ner_extractor(self) -> None:
        get_attr = ['PERSON', 'ORG', 'GPE']
        extractor = SpacyNerExtractor(extractor_name='spacy_ner_extractor')
        text = 'Napoléon Bonaparte was a French statesman and military leader who rose to prominence during the French Revolution and led several successful campaigns during the French Revolutionary Wars. As Napoleon, he was Emperor of the French from 1804 until 1814, and again briefly in 1815 during the Hundred Days. Napoleon dominated European and global affairs for more than a decade while leading France against a series of coalitions in the Napoleonic Wars. He won most of these wars and the vast majority of his battles, building a large empire that ruled over continental Europe before its final collapse in 1815. He is considered one of the greatest commanders in history, and his wars and campaigns are studied at military schools worldwide. Napoleon\'s political and cultural legacy has endured as one of the most celebrated and controversial leaders in human history.'
        extracted = list()
        results = extractor.extract(text, get_attr=get_attr)

        for i in results:
            extracted_value = {
                'value': i.value,
                'start_char': i.provenance['start_char'],
                'end_char': i.provenance['end_char'],
                'start_token': i.provenance['start_token'],
                'end_token': i.provenance['end_token'],
                'tag': i.tag
            }
            extracted.append(extracted_value)
        expected = [{'value': 'Napoléon Bonaparte', 'start_char': 0, 'end_char': 18, 'start_token': 0, 'end_token': 2,
                     'tag': 'PERSON'},
                    {'value': 'Napoleon', 'start_char': 192, 'end_char': 200, 'start_token': 29, 'end_token': 30,
                     'tag': 'ORG'},
                    {'value': 'Napoleon', 'start_char': 304, 'end_char': 312, 'start_token': 52, 'end_token': 53,
                     'tag': 'ORG'},
                    {'value': 'France', 'start_char': 388, 'end_char': 394, 'start_token': 65, 'end_token': 66,
                     'tag': 'GPE'},
                    {'value': 'Napoleon', 'start_char': 738, 'end_char': 746, 'start_token': 129, 'end_token': 130,
                     'tag': 'ORG'}]
        result_count = 0
        while result_count < len(extracted):
            self.assertEqual(extracted[result_count], expected[result_count])
            result_count += 1


if __name__ == '__main__':
    unittest.main()
