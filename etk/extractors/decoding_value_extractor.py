from typing import List
from etk.extractor import Extractor, InputType
from etk.extraction import Extraction
from etk.etk_exceptions import ExtractorError


class DecodingValueExtractor(Extractor):

    def __init__(self,
                 decoding_dict: dict,
                 extractor_name: str,
                 default_action: str='delete',
                 case_sensitive: bool=False,
                 strip_key: bool=True,
                 strip_value: bool=False,
                 ) -> None:
        """

        Args:
            decoding_dict: dict -> a python dictionary for decoding values
            extractor_name: str -> extractor name
            default_action: enum['delete'] ->  what if the value not matched in dictionary
            case_sensitive: bool -> matching the key and value strictly or ignore cases
            strip_key: bool -> strip key and value for matching or not
            strip_value: bool -> return the striped value if matched or the original value
        """
        Extractor.__init__(self,
                           input_type=InputType.TEXT,
                           category="dictionary",
                           name=extractor_name)
        if case_sensitive and not strip_key:
            self.decoding_dict = decoding_dict
        else:
            new_dict = {}
            if not strip_key:   # not case_sensitive, ignore cases
                for k in decoding_dict:
                    new_dict[k.lower()] = decoding_dict[k]
            elif case_sensitive:   # strip key
                for k in decoding_dict:
                    new_dict[k.strip()] = decoding_dict[k]
            else:   # ignore case AND strip key
                for k in decoding_dict:
                    new_dict[k.lower().strip()] = decoding_dict[k]
            self.decoding_dict = new_dict

        self.case_sensitive = case_sensitive
        self.default_action = default_action
        self.strip_key = strip_key
        self.strip_value = strip_value

        self.joiner = " "

    def extract(self, value: str) -> List[Extraction]:
        """

        Args:
            value: str -> the value to be decode

        Returns: List[Extraction] -> actually a single Extraction wrapped in a list if there is a match

        """

        to_match = value.lower() if not self.case_sensitive else value
        to_match = to_match.strip() if self.strip_key else to_match

        if to_match in self.decoding_dict:
            extraction = self.wrap_result(self.decoding_dict[to_match], value)
            return [extraction] if extraction else list()
        else:
            if self.default_action == 'delete':
                return list()

        return list()

    def wrap_result(self, value: str, original_key: str) -> Extraction or None:
        """

        Args:
            value: the decoded value
            original_key: the original string value to be decode

        Returns: an Extraction if everything goes well

        """
        try:
            value = value.strip() if self.strip_value else value
            e = Extraction(value, self.name, start_char=0, end_char=len(str(value)))
            return e
        except Exception as e:
            print('fail to wrap dictionary extraction: ', original_key, value)
            raise ExtractorError('Exception: ' + str(e))
