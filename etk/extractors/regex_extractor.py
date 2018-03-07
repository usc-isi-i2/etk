from typing import List
from enum import Enum, auto
from etk.extractor import Extractor, InputType
from etk.etk_extraction import Extraction
import re

class MatchMode(Enum):
    MATCH = auto(),
    SEARCH = auto()

class RegexExtractor(Extractor):
    """
    Extract using Python regular expressions.
    """
    def __init__(self,
                 pattern: str,
                 extractor_name: str,
                 flags=0,
                 ) -> None:
        Extractor.__init__(self,
                           input_type=InputType.TEXT,
                           category="regex",
                           name=extractor_name)
        self._compiled_regex = re.compile(pattern, flags)

        """
        Extracts information from a text using the given regex.
        If the pattern has no groups, it returns a list with a single Extraction.
        If the pattern has groups, it returns a list of Extraction, one for each group.
        Each extraction records the start and end char positions of matches.
        Args:
            text (str): the text to extract from.
            flags (): flags given to search or match.
            mode (): whether to use re.search() or re.match().
        Returns: the List(Extraction) or the empty list if there are no matches.
        """
        

    def extract(self, text: str, flags=0, mode: MatchMode=MatchMode.SEARCH) -> List[Extraction]:
        if mode == MatchMode.MATCH:
            matches = self._compiled_regex.match(text, flags)
        elif mode == MatchMode.SEARCH:
            matches = self._compiled_regex.search(text, flags)

        return self.wrap_result(matches)
        raise NotImplementedError


    def wrap_result(self, matches: object) -> List[Extraction]:
        res = list()
        # check if the pattern has groups
        groups = matches.groups()
        if groups:
            for i in range(1, len(groups)+1):
                res.append(self.wrap_extraction(i, matches))
        else:
            res.append(self.wrap_extraction(0, matches))

        return res
        raise NotImplementedError


    def wrap_extraction(self, group_idx: int, matches: object) -> Extraction:
        start, end = matches.start(group_idx), matches.end(group_idx)
        text = matches.group(group_idx)
        e = Extraction(value = text, extractor_name = self.name,\
                        start_char = start, end_char = end)
        return e
        raise NotImplementedError

