import json
from etk_extraction import Extractable
import jsonpath_rw
from segment import SegmentCollection, Segment


class Document(Extractable):
    """
        This class wraps raw CDR documents and provides a convenient API for ETK
        to query elements of the document and to update the document with the results
        of extractors.
        """

    def __init__(self, cdr_document, tokenizer=None):
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
        if tokenizer:
            self.tokenizer = tokenizer
        self.segments = None

    def select_segments(self, json_path):
        """
        Dereferences the json_path inside the document and returns the selected elements.
        This method should compile and cache the compiled json_path in case the same path
        is reused by multiple extractors.

        Args:
            json_path (str): a valid JSON path specification.

        Returns: A Segments object that contains the elements selected by the json_path.
        """
        path = jsonpath_rw.parse(json_path)
        matches = path.find(self.cdr_document)
        if not matches:
            return None
        else:
            segments = SegmentCollection()
            for a_match in matches:
                this_segment = Segment(str(a_match.full_path), a_match.value)
                segments.add(this_segment)

        self.segments = segments
        return segments

    def invoke_extractor(self,
                         extractor,
                         extractable):
        pass
        """
        Invoke the extractor for each Segment, accumulating all the extractions in an ExtractionCollection.
    
        Args:
            extractor (Extractor):
            extractable (Extractable):
    
        Returns: ExtractionCollection, containing all the extractions.

        """
        # pseudo-code:

        # Need to handle the case of multiple *extractable
        # If they are all primitive or singletons, it is simple,
        #   just invoke once passing the arguments to the extractor.
        # If they are not primitive or singletons, line them up inventing empty Extractable to fill the gaps:
        #   e.g., [A B C], [K, L], [X, Y, Z] => e(A, K, X), e(B, L, Y), e(C, EMPTY, X)
        # ec = ExtractionCollection()
        # # Need to test for PrimitiveExtractable or ExtractableCollection as items is only defined for collections.
        # for primitive_extractable in extractable.items():
        #     if extractor.input_type() == Extractor.InputType.TOKENS:
        #         tokens = self.get_tokens(primitive_extractable, tokenizer=extractor.preferred_tokenizer())
        #         if tokens:
        #             # put the tokens in the extractable so that the extractor can get to them.
        #             primitive_extractable.set_tokens(tokens)
        #     ec.add_extractions(extractor.extract(primitive_extractable))
        #
        # # record provenance:
        # for e in ec.extractions():
        #     pass
            # add a ProvenanceRecord for the extraction
            # the prov record for each extraction should point to all extractables:
            # If the extractables are segments, put them in the "input_segments"
            # If the extractables are extractions, put the prov ids of the extractions in "input_extractions"

