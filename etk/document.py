import json
from etk.etk_extraction import Extractable, Extraction
from etk.segment import Segment
from etk.extractor import Extractor
from typing import List


class Document(Extractable):
    """
        This class wraps raw CDR documents and provides a convenient API for ETK
        to query elements of the document and to update the document with the results
        of extractors.
        """

    def __init__(self, cdr_document, tokenizer) -> None:
        """
        Wrapper object for CDR documents.

        Args:
            cdr_document (JSON): the raw CDR document received in ETK.
            tokenizer (Tokenizer): the default tokenizer for creating tokens.

        Returns: the wrapped CDR document

        """
        Extractable.__init__(self)
        self.cdr_document = json.loads(cdr_document)
        self._value = self.cdr_document
        self.default_tokenizer = tokenizer

    def select_segments(self, path) -> List[Segment]:
        """
        Dereferences the json_path inside the document and returns the selected elements.
        This method should compile and cache the compiled json_path in case the same path
        is reused by multiple extractors.

        Args:
            path (jsonpath_expr): a valid parsed JSON path.

        Returns: A list of Segments object that contains the elements selected by the json path.
        """
        matches = path.find(self.cdr_document)
        segments = []
        for a_match in matches:
            this_segment = Segment(str(a_match.full_path), a_match.value)
            segments.append(this_segment)

        return segments

    def invoke_extractor(self, extractor, extractable, tokenizer=None) -> List[Extraction]:
        """
        Invoke the extractor for each Segment, accumulating all the extractions in an ExtractionCollection.

        Args:
            extractor (Extractor):
            extractable (extractable):
            tokenizer: user can pass custom tokenizer

        Returns: List of Extraction, containing all the extractions.

        """
        if not tokenizer:
            tokenizer = self.default_tokenizer
        extractions = []
        if extractor.input_type == Extractor.InputType.TOKENS:
            tokens = extractable.get_tokens(tokenizer)
            if tokens:
                extracted_results = extractor.extract(tokens)
                for a_result in extracted_results:
                    this_extraction = Extraction(a_result)
                    extractions.append(this_extraction)

        return extractions
        # record provenance:
        #  add a ProvenanceRecord for the extraction
        #  the prov record for each extraction should point to all extractables:
        #  If the extractables are segments, put them in the "input_segments"
        #  If the extractables are extractions, put the prov ids of the extractions in "input_extractions"
