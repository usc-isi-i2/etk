from typing import List
from etk.extractor import Extractor, InputType
from etk.extraction import Extraction
from etk.etk_exceptions import ExtractorError


class DecodingValueExtractor(Extractor):
    """
    **Description**
        This class takes a 'decoding_dict' as reference, decoding the input text based on the 'decoding_dict'

    Examples:
        ::

            decoding_dict = {
            'CA': 'California',
            'ny': 'New York',
            'AZ': ' Arizona',
            ' TX ': 'Texas',
            ' fl': 'Florida',
            }
            decoding_value_extractor = DecodingValueExtractor(decoding_dict=decoding_dict,
                                                            extractor_name='default_decoding',
                                                            case_sensitive=True,
                                                            strip_key=False,
                                                            strip_value=True)
            decoding_value_extractor.extract(value=value_to_be_decoded)

    """
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
            self._decoding_dict = decoding_dict
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
            self._decoding_dict = new_dict

        self._case_sensitive = case_sensitive
        self._default_action = default_action
        self._strip_key = strip_key
        self._strip_value = strip_value

        self._joiner = " "

    def extract(self, value: str) -> List[Extraction]:
        """

        Args:
            value (str): the value to be decode

        Returns:
            List[Extraction]: actually a single Extraction wrapped in a list if there is a match

        """

        to_match = value.lower() if not self._case_sensitive else value
        to_match = to_match.strip() if self._strip_key else to_match

        if to_match in self._decoding_dict:
            extraction = self._wrap_result(self._decoding_dict[to_match], value)
            return [extraction] if extraction else list()
        else:
            if self._default_action == 'delete':
                return list()

        return list()

    def _wrap_result(self, value: str, original_key: str) -> Extraction or None:
        """

        Args:
            value: the decoded value
            original_key: the original string value to be decode

        Returns: an Extraction if everything goes well

        """
        try:
            value = value.strip() if self._strip_value else value
            e = Extraction(value, self.name, start_char=0, end_char=len(str(value)))
            return e
        except Exception as e:
            print('fail to wrap dictionary extraction: ', original_key, value)
            raise ExtractorError('Exception: ' + str(e))
