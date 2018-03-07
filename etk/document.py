from jsonpath_rw import jsonpath
from typing import List, Dict

from etk.etk_extraction import Extractable, Extraction
from etk.extractor import Extractor, InputType
from etk.segment import Segment
from etk.tokenizer import Tokenizer


class Document(Extractable):
    """
        This class wraps raw CDR documents and provides a convenient API for ETK
        to query elements of the document and to update the document with the results
        of extractors.
        """
    def __init__(self, etk, cdr_document: Dict) -> None:

        """
        Wrapper object for CDR documents.

        Args:
            etk (ETK): embed the etk object so that docs have access to global info.
            cdr_document (JSON): the raw CDR document received in ETK.

        Returns: the wrapped CDR document

        """
        Extractable.__init__(self)
        self.etk = etk
        self.cdr_document = cdr_document
        self._value = cdr_document
        self.default_tokenizer = etk.default_tokenizer

    def select_segments(self, jsonpath: str) -> List[Segment]:
        """
        Dereferences the json_path inside the document and returns the selected elements.
        This method should compile and cache the compiled json_path in case the same path
        is reused by multiple extractors.

        Args:
            jsonpath (str): a valid JSON path.

        Returns: A list of Segments object that contains the elements selected by the json path.
        """
        path = self.etk.invoke_parser(jsonpath)
        matches = path.find(self.cdr_document)
        segments = []
        for a_match in matches:
            this_segment = Segment(str(a_match.full_path), a_match.value)
            segments.append(this_segment)

        return segments

    def invoke_extractor(self, extractor: Extractor, extractable: Extractable = None, tokenizer: Tokenizer = None,
                         joiner: str = "  ", **options) -> List[Extraction]:

        """
        Invoke the extractor on the given extractable, accumulating all the extractions in a list.

        Args:
            extractor (Extractor):
            extractable (extractable):
            tokenizer: user can pass custom tokenizer if extractor wants token
            joiner: user can pass joiner if extractor wants text
            options: user can pass arguments as a dict to the extract() function of different extractors

        Returns: List of Extraction, containing all the extractions.

        """
        if not extractable:
            extractable = self

        if not tokenizer:
            tokenizer = self.default_tokenizer

        extracted_results = list()

        if extractor.input_type == InputType.TOKENS:
            tokens = extractable.get_tokens(tokenizer)
            if tokens:
                extracted_results = extractor.extract(tokens, **options)

        elif extractor.input_type == InputType.TEXT:
            text = extractable.get_string(joiner)
            if text:
                extracted_results = extractor.extract(text, **options)

        elif extractor.input_type == InputType.OBJECT:
            extracted_results = extractor.extract(extractable.value, **options)

        elif extractor.input_type == InputType.HTML:
            extracted_results = extractor.extract(extractable.value['raw_content'], **options)


        # TODO: the reason that extractors must return Extraction objects is so that
        # they can communicate back the provenance.

        return extracted_results
        # record provenance:
        #  add a ProvenanceRecord for the extraction
        #  the prov record for each extraction should point to all extractables:
        #  If the extractables are segments, put them in the "input_segments"
        #  If the extractables are extractions, put the prov ids of the extractions in "input_extractions"
