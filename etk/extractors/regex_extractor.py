from typing import List
from enum import Enum, auto
from etk.extractor import Extractor, InputType
from etk.extraction import Extraction
import re
import collections


class MatchMode(Enum):
    MATCH = auto(),
    SEARCH = auto(),
    FINDALL = auto(),
    SPLIT = auto()


class RegexExtractor(Extractor):
    """
    Extract using Python regular expressions.
    """

    def __init__(self,
                 pattern: str,
                 extractor_name: str,
                 flags=0,
                 general_tag: str = None
                 ) -> None:
        Extractor.__init__(self,
                           input_type=InputType.TEXT,
                           category="regex",
                           name=extractor_name)

        self._compiled_regex = re.compile(pattern, flags)
        self._general_tag = general_tag

        self._match_functions = {
            MatchMode.MATCH: self._compiled_regex.match,
            MatchMode.SEARCH: self._compiled_regex.search,
            MatchMode.FINDALL: self._compiled_regex.finditer,
            MatchMode.SPLIT: self._compiled_regex.split
        }

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

    @property
    def general_tag(self):
        return self._general_tag

    def extract(self, text: str, flags=0, mode: MatchMode = MatchMode.FINDALL) -> List[Extraction]:
        match_func = self._match_functions[mode]
        matches = match_func(text, flags)
        return self.wrap_result(matches)

    # wrap the re return object to list of extraction
    def wrap_result(self, matches: object) -> List[Extraction]:
        res = list()
        # matches are result of split()
        if isinstance(matches, list):
            return self.wrap_split_extraction(matches)

        # matches are result of finditer()
        elif isinstance(matches, collections.Iterable):
            for match in matches:
                es = self.wrap_result(match)
                res.extend(es)

        # single match
        else:
            # check if the pattern has groups
            groups = matches.groups()
            if groups:
                for i in range(1, len(groups) + 1):
                    res.append(self.wrap_extraction(i, matches))
            else:
                res.append(self.wrap_extraction(0, matches))
        return res

    def wrap_split_extraction(self, items: List[str]) -> List[Extraction]:
        res = list()
        start = 0
        for item in items:
            end = start + len(item)
            e = Extraction(value=item, extractor_name=self.name, start_char=start, end_char=end)
            res.append(e)
            start = end
        return res

    def wrap_extraction(self, group_idx: int, matches: object) -> Extraction:
        start, end = matches.start(group_idx), matches.end(group_idx)
        text = matches.group(group_idx)
        e = Extraction(value=text, extractor_name=self.name, \
                       start_char=start, end_char=end, tag=self.general_tag)
        return e
