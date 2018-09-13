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
    **Description**
        A wrapper for Python regular expressions.

    Examples:
        ::

            pattern = "some_pattern"
            regex_extractor = RegexExtractor(pattern=pattern,
                                            flags=re.IGNORECASE)
            regex_extractor.extract(text=input_doc,
                                    flags=re.M,
                                    MatchMode=MatchMode.SEARCH
                                    )

    """

    def __init__(self,
                 pattern: str,
                 extractor_name: str='regex extractor',
                 flags=0,
                 general_tag: str=None
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

    @property
    def general_tag(self):
        return self._general_tag

    def extract(self, text: str, flags=0, mode: MatchMode = MatchMode.FINDALL) -> List[Extraction]:

        """

            Extracts information from a text using the given regex.
            If the pattern has no groups, it returns a list with a single Extraction.
            If the pattern has groups, it returns a list of Extraction, one for each group.
            Each extraction records the start and end char positions of matches.

        Args:
            text (str): the text to extract from.
            flags (enum['a', 'i', 'L', 'm', 's', 'u', 'x']): flags given to search or match. The value should be one \
                or more letters from the set 'a', 'i', 'L', 'm', 's', 'u', 'x'.) The group matches the empty string; \
                the letters set the corresponding flags: re.A (ASCII-only matching), re.I (ignore case), re.L (locale dependent),\
                re.M (multi-line), re.S (dot matches all), re.U (Unicode matching), and re.X (verbose), for the entire \
                regular expression.
            mode (enum[MatchMode.MATCH, MatchMode.SEARCH, MatchMode.FINDALL, MatchMode.SPLIT]): whether to use re.search() or re.match().

        Returns:
            List(Extraction): the list of extraction or the empty list if there are no matches.

        """

        match_func = self._match_functions[mode]
        matches = match_func(text, flags)
        return self._wrap_result(matches)

    # wrap the re return object to list of extraction
    def _wrap_result(self, matches: object) -> List[Extraction]:
        res = list()
        # matches are result of split()
        if isinstance(matches, list):
            return self._wrap_split_extraction(matches)

        # matches are result of finditer()
        elif isinstance(matches, collections.Iterable):
            for match in matches:
                es = self._wrap_result(match)
                res.extend(es)

        # single match
        else:
            # check if the pattern has groups
            groups = matches.groups()
            if groups:
                for i in range(1, len(groups) + 1):
                    res.append(self._wrap_extraction(i, matches))
            else:
                res.append(self._wrap_extraction(0, matches))
        return res

    def _wrap_split_extraction(self, items: List[str]) -> List[Extraction]:
        res = list()
        start = 0
        for item in items:
            end = start + len(item)
            e = Extraction(value=item, extractor_name=self.name, start_char=start, end_char=end)
            res.append(e)
            start = end
        return res

    def _wrap_extraction(self, group_idx: int, matches: object) -> Extraction:
        start, end = matches.start(group_idx), matches.end(group_idx)
        text = matches.group(group_idx)
        e = Extraction(value=text, extractor_name=self.name, \
                       start_char=start, end_char=end, tag=self.general_tag)
        return e
