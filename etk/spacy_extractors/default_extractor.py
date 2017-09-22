# coding: utf-8

import re
import spacy
from string import punctuation

# Constants'
SPACY_ENTITIES = [
    'PERSON',
    'NORP',
    'FACILITY',
    'ORG',
    'GPE',
    'LOC',
    'PRODUCT',
    'EVENT',
    'WORK_OF_ART',
    'LANGUAGE',
    'DATE',
    'TIME',
    'PERCENT',
    'MONEY',
    'QUANTITY',
    'ORDINAL',
    'CARDINAL'
]
'''
PERSON  People, including fictional.
NORP    Nationalities or religious or political groups.
FACILITY    Buildings, airports, highways, bridges, etc.
ORG Companies, agencies, institutions, etc.
GPE Countries, cities, states.
LOC Non-GPE locations, mountain ranges, bodies of water.
PRODUCT Objects, vehicles, foods, etc. (Not services.)
EVENT   Named hurricanes, battles, wars, sports events, etc.
WORK_OF_ART Titles of books, songs, etc.
LANGUAGE    Any named language.
---
DATE    Absolute or relative dates or periods.
TIME    Times smaller than a day.
PERCENT Percentage, including "%".
MONEY   Monetary values, including unit.
QUANTITY    Measurements, as of weight or distance.
ORDINAL "first", "second", etc.
CARDINAL    Numerals that do not fall under another type.

'''


class DefaultExtractor(object):

    def __init__(self):
        self.spacy_to_etk_mapping = dict()

    def add_mapping(self, etk_entity_label, spacy_entity_label):
        self.spacy_to_etk_mapping[spacy_entity_label] = etk_entity_label

    def set_mapping(self, mapping):
        self.spacy_to_etk_mapping = mapping

    @staticmethod
    def process_entity(ent, spacy_to_etk_mapping, doc):
        ent_processed = dict()

        start = ent.start
        end = ent.end

        while start < end:
            start_text = doc[start].text
            end_text = doc[end - 1].text

            start_text = start_text.strip().strip(punctuation).strip()
            end_text = end_text.strip().strip(punctuation).strip()

            if len(start_text) == 0:
                start += 1
            elif len(end_text) == 0:
                end -= 1
            else:
                break

        ent_processed['text'] = doc[start:end]
        ent_processed['start'] = start
        ent_processed['end'] = end
        ent_processed['semantic_type'] = spacy_to_etk_mapping.get(
            ent.label_, None)
        return ent_processed

    @staticmethod
    def extract(doc, spacy_to_etk_mapping):
        extracted_entities = dict()
        for ent in doc.ents:
            # Process the entity
            ent_processed = DefaultExtractor.process_entity(
                ent, spacy_to_etk_mapping, doc)

            if ent_processed['semantic_type'] is not None and len(ent_processed['text']) != 0:
                extracted_entity = dict()
                extracted_entity['context'] = {
                    'start': ent_processed['start'],
                    'end': ent_processed['end']
                }

                extracted_entity['value'] = str(ent_processed['text'])

                extracted_entities[ent_processed['semantic_type']] = extracted_entities.get(
                    ent_processed['semantic_type'], list())
                extracted_entities[ent_processed['semantic_type']].append(
                    extracted_entity)

        return extracted_entities


def main():
    de = DefaultExtractor()

    de.add_mapping('e1', 'GPE')
    de.add_mapping('e2', 'ORG')
    de.add_mapping('e3', 'PERSON')
    de.add_mapping('e4', 'NORP')

    # entities_list = {'l1'}

    nlp = spacy.load('en')
    doc = nlp(u'Russia will be required to close its consulate general in San Francisco, the chancery annex in Washington and the consular annex in New York, the State Department announced. The deadline is Saturday. A senior administration official would not say hxow many Russian staffers were affected but noted they will not be required to leave the country. The official also did not say if the Russian missions employ any Americans. The move was the latest tit-for-tat action in worsening relations between Washington and Moscow, despite President Trump\'s expressions of friendliness toward President Vladimir Putin. Angered over a package of congressionally mandated economic sanctions, Russia had ordered the U.S. to cut its staff in Russia by around two - thirds, to 455. The administration official would not give an exact figure of how many staffers left Russia, or say how many of those cut were Americans, who will be transferred, or Russians, who will most likely be fired.')

    print de.extract(doc, de.spacy_to_etk_mapping)


if __name__ == '__main__':
    main()
