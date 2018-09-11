from etk.extractor import Extractor, InputType
from etk.extraction import Extraction
from spacy.matcher import Matcher
from spacy.attrs import LIKE_EMAIL
from typing import List
import copy


FILTER_PROVIDER = ["noon", "no"]


class EmailExtractor(Extractor):
    """
    **Description**
        This class uses spaCy Matcher and takes spaCy predefined 'LIKE_EMAIL' pattern to extract email address.
        More information: https://spacy.io/api/matcher#add

    Examples:
        ::

            email_extractor = EmailExtractor(...)
            email_extractor.extract(text=input_doc,...)

    """
    def __init__(self,
                 nlp,
                 tokenizer,
                 extractor_name: str) -> None:
        """
        Initialize the extractor, storing the rule information and construct spacy rules
        Args:
            nlp:
            tokenizer: Tokenizer
            extractor_name: str

        Returns:
        """
        Extractor.__init__(self,
                           input_type=InputType.TEXT,
                           category="build_in_extractor",
                           name=extractor_name)

        self._nlp = copy.deepcopy(nlp)
        self._like_email_matcher = Matcher(self._nlp.vocab)
        self._tokenizer = tokenizer

    def _load_email_matcher(self):
        self._like_email_matcher.add("Email", None, [{LIKE_EMAIL: True}])

    def extract(self, text: str) -> List[Extraction]:
        """

        Args:
            text (str): The input source to be processed

        Returns:
            List[Extraction]: The list of extractions returned by EmailExtractor

        """

        result = []
        first_phase_doc = self._nlp(text)
        self._load_email_matcher()
        like_email_matches = self._like_email_matcher(first_phase_doc)

        like_emails_filtered = []
        for match_id, start, end in like_email_matches:
            span = first_phase_doc[start:end]
            if self._check_domain(self._tokenizer.tokenize(span.text)):
                like_emails_filtered.append((span.text, span[0].idx, span[-1].idx + len(span[-1])))

        non_space_emails = self._get_non_space_email(first_phase_doc)

        emails = set(like_emails_filtered).union(non_space_emails)

        for email in emails:
            result.append(Extraction(
                value=email[0],
                extractor_name=self.name,
                start_char=email[1],
                end_char=email[2])
            )

        return result

    @staticmethod
    def _check_domain(tokens) -> bool:
        """
        Check if the email provider should be filtered
        Args:
            tokens:

        Returns: Bool
        """

        idx = None
        for e in tokens:
            if e.text == "@":
                idx = e.i
                break
        if not idx or tokens[idx+1].text in FILTER_PROVIDER:
            return False
        else:
            return True

    def _get_non_space_email(self, doc) -> List:
        """
        Deal with corner case that there is "email" string in text and no space around it
        Args:
            doc: List[Token]

        Returns: Bool
        """
        result_lst = []
        for e in doc:
            if "mail:" in e.text.lower():
                idx = e.text.lower().index("mail:") + 5
                value = e.text[idx:]
                tmp_doc = self._nlp(value)
                tmp_email_matches = self._like_email_matcher(tmp_doc)
                for match_id, start, end in tmp_email_matches:
                    span = tmp_doc[start:end]
                    if self._check_domain(self._tokenizer.tokenize(span.text)):
                        result_lst.append((span.text, idx+e.idx, idx+e.idx+len(value)))

        return result_lst
