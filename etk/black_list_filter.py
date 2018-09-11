from typing import List
from etk.extraction import Extraction


class BlackListFilter(object):
    def __init__(self, black_list: List[str]) -> None:
        self.black_list = black_list

    def filter(self, extractions, case_sensitive=False) -> List[Extraction]:
        """filters out the extraction if extracted value is in the blacklist"""
        filtered_extractions = []
        if not isinstance(extractions, list):
            extractions = [extractions]

        for extraction in extractions:
            if case_sensitive:
                try:
                    if extraction.value.lower() not in self.black_list:
                        filtered_extractions.append(extraction)
                except Exception as e:
                    print('Error in BlackListFilter: {} while filtering out extraction: {}'.format(e, extraction.value))
                    # most likely it s a unicode character which is messing things up, return it
                    filtered_extractions.append(extraction)
            else:
                if extraction.value not in self.black_list:
                    filtered_extractions.append(extraction)
        return filtered_extractions
