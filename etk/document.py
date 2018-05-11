from typing import List, Dict
from bs4 import BeautifulSoup
from etk.extraction_provenance_record import ExtractionProvenanceRecord
from etk.extraction import Extractable, Extraction
from etk.extractor import Extractor, InputType
from etk.segment import Segment
from etk.tokenizer import Tokenizer
from etk.knowledge_graph import KnowledgeGraph
from etk.etk_exceptions import ErrorPolicy, ExtractorValueError
import warnings


class Document(Segment):
    """
        This class wraps raw CDR documents and provides a convenient API for ETK
        to query elements of the document and to update the document with the results
        of extractors.
        """
    def __init__(self, etk, cdr_document: Dict, mime_type, url, doc_id=None) -> None:

        """
        Wrapper object for CDR documents.

        Args:
            etk (ETK): embed the etk object so that docs have access to global info.
            cdr_document (JSON): the raw CDR document received in ETK.

        Returns: the wrapped CDR document

        """
        Segment.__init__(self, json_path="$", _value=cdr_document)
        self.etk = etk
        self.cdr_document = cdr_document
        self.mime_type = mime_type
        self.url = url
        if doc_id:
            self.cdr_document["doc_id"] = doc_id
        self.extraction_provenance_records = []
        self.extraction_provenance_id_index = 0
        if self.etk.kg_schema:
            self.kg = KnowledgeGraph(self.etk.kg_schema, self)
        else:
            self.kg = None
            self.etk.log("Schema not found.", "warning", self.doc_id, self.url)

    @property
    def document(self):
        """
        Still thinking about this, having the parent doc inside each extractable is convenient to avoid
        passing around the parent doc to all methods.

        Returns: the parent Document
        """
        return self

    def select_segments(self, jsonpath: str) -> List[Segment]:
        """
        Dereferences the json_path inside the document and returns the selected elements.
        This method should compile and cache the compiled json_path in case the same path
        is reused by multiple extractors.

        Args:
            jsonpath (str): a valid JSON path.

        Returns: A list of Segments object that contains the elements selected by the json path.
        """
        path = self.etk.parse_json_path(jsonpath)
        matches = path.find(self.cdr_document)

        segments = []
        for a_match in matches:
            this_segment = Segment(str(a_match.full_path), a_match.value, self)
            segments.append(this_segment)

        return segments

    def extract(self, extractor: Extractor, extractable: Extractable = None, tokenizer: Tokenizer = None,
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
            tokenizer = self.etk.default_tokenizer

        extracted_results = list()

        if extractor.input_type == InputType.TOKENS:
            if self.etk.error_policy == ErrorPolicy.PROCESS:
                if isinstance(extractable.value, list):
                    self.etk.log(
                        "Extractor needs tokens, tokenizer needs string to tokenize, got list, converting to string",
                        "warning", self.doc_id, self.url)
                    warnings.warn(
                        "Extractor needs tokens, tokenizer needs string to tokenize, got list, converting to string")
                elif isinstance(extractable.value, dict):
                    self.etk.log(
                        "Extractor needs tokens, tokenizer needs string to tokenize, got dict, converting to string",
                        "warning", self.doc_id, self.url)
                    warnings.warn(
                        "Extractor needs tokens, tokenizer needs string to tokenize, got dict, converting to string")
                tokens = extractable.get_tokens(tokenizer)
                if tokens:
                    extracted_results = extractor.extract(tokens, **options)
            else:
                raise ExtractorValueError(
                    "Extractor needs string, tokenizer needs string to tokenize, got " + str(type(extractable.value)))

        elif extractor.input_type == InputType.TEXT:
            if self.etk.error_policy == ErrorPolicy.PROCESS:
                if isinstance(extractable.value, list):
                    self.etk.log("Extractor needs string, got extractable value as list, converting to string",
                                 "warning", self.doc_id, self.url)
                    warnings.warn("Extractor needs string, got extractable value as list, converting to string")
                elif isinstance(extractable.value, dict):
                    self.etk.log("Extractor needs string, got extractable value as dict, converting to string",
                                 "warning", self.doc_id, self.url)
                    warnings.warn("Extractor needs string, got extractable value as dict, converting to string")
                text = extractable.get_string(joiner)
                if text:
                    extracted_results = extractor.extract(text, **options)
            else:
                raise ExtractorValueError("Extractor needs string, got " + str(type(extractable.value)))

        elif extractor.input_type == InputType.OBJECT:
            extracted_results = extractor.extract(extractable.value, **options)

        elif extractor.input_type == InputType.HTML:
            if bool(BeautifulSoup(extractable.value, "html.parser").find()):
                extracted_results = extractor.extract(extractable.value, **options)
            else:
                raise ExtractorValueError("Extractor needs HTML, got non HTML string")

        try:
            jsonPath = extractable.full_path
            _document = extractable.document
        except AttributeError:
            jsonPath = None
            _document = None

        for e in extracted_results:
            extraction_provenance_record: ExtractionProvenanceRecord = ExtractionProvenanceRecord(
                self.extraction_provenance_id_index, jsonPath, e.provenance["extractor_name"],
                e.provenance["start_char"], e.provenance["end_char"], e.provenance["confidence"], _document,
                extractable.prov_id)

            e.prov_id = self.extraction_provenance_id_index # for the purpose of provenance hierarrchy tracking
            self.extraction_provenance_id_index = self.extraction_provenance_id_index + 1
            self.create_provenance(extraction_provenance_record)

        return extracted_results

    @property
    def doc_id(self):
        """
        Returns: the doc_id of the CDR document

        """
        return self.cdr_document.get("doc_id")

    @doc_id.setter
    def doc_id(self, new_doc_id):
        """

        Args:
           new_doc_id ():

        Returns:

        """
        self.cdr_document["doc_id"] = new_doc_id

    def create_provenance(self, extractionProvenanceRecord: ExtractionProvenanceRecord) -> None:
        if "provenances" not in self.cdr_document:
            self.cdr_document["provenances"] = []
        self.cdr_document["provenances"].append(self.get_dict_extraction_provenance(extractionProvenanceRecord))

    def get_dict_extraction_provenance(self, extractionProvenanceRecord: ExtractionProvenanceRecord):
        dict = {}
        dict["@type"] = "extraction_provenance_record"
        dict["@id"] = extractionProvenanceRecord.id
        dict["method"] = extractionProvenanceRecord.method
        dict["confidence"] = extractionProvenanceRecord.extraction_confidence
        if extractionProvenanceRecord.origin_record.full_path is not None:
            dict["origin_record"] = []
            origin_dict = {}
            origin_dict["path"] = extractionProvenanceRecord.origin_record.full_path
            origin_dict["start_char"] = extractionProvenanceRecord.origin_record.start_char
            origin_dict["end_char"] = extractionProvenanceRecord.origin_record.end_char
            dict["origin_record"].append(origin_dict)
        if extractionProvenanceRecord.parent_extraction_provenance is not None:
            dict["provenance_id"] = extractionProvenanceRecord.parent_extraction_provenance
        return dict

    def insert_kg_into_cdr(self):
        if self.kg and self.kg.value:
            self.cdr_document["knowledge_graph"] = self.kg.value
