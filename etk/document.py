from typing import List, Dict
from bs4 import BeautifulSoup
from etk.extraction_provenance_record import ExtractionProvenanceRecord
from etk.extraction import Extractable, Extraction
from etk.extractor import Extractor, InputType
from etk.segment import Segment
from etk.tokenizer import Tokenizer
from etk.knowledge_graph import KnowledgeGraph
from etk.utilities import Utility
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
        Segment.__init__(self, json_path="$", _value=cdr_document, _document=self)
        self.etk = etk
        self.cdr_document = cdr_document
        self.mime_type = mime_type
        self.url = url
        if doc_id:
            self.cdr_document["doc_id"] = doc_id
        self.extraction_provenance_records = list()
        if self.etk.kg_schema:
            self.kg = KnowledgeGraph(self.etk.kg_schema, self.etk.ontology, self)
        else:
            self.kg = None
            if not self.etk.kg_schema:
                self.etk.log("Schema not found.", "warning", self.doc_id, self.url)
        self._provenance_id_index = 0
        self._provenances = dict()
        self._jsonpath_provenances = dict()
        self._kg_provenances = dict()

    @property
    def provenance_id_index(self) -> int:
        return self._provenance_id_index

    @property
    def provenances(self) -> Dict:
        return self._provenances

    @property
    def jsonpath_provenances(self) -> Dict:
        return self._jsonpath_provenances

    @property
    def kg_provenances(self) -> Dict:
        return self._kg_provenances

    def provenance_id_index_incrementer(self):
        self._provenance_id_index += 1

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

        segments = list()
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
                # raise ExtractorValueError("Extractor needs string, got " + str(type(extractable.value)))
                # TODO: Yixiang - needs to be handled properly
                pass

        elif extractor.input_type == InputType.OBJECT:
            extracted_results = extractor.extract(extractable.value, **options)

        elif extractor.input_type == InputType.HTML:
            if bool(BeautifulSoup(extractable.value, "html.parser").find()):
                extracted_results = extractor.extract(extractable.value, **options)
            else:
                # raise ExtractorValueError("Extractor needs HTML, got non HTML string")
                # TODO: Yixiang - needs to be handled properly
                pass

        try:
            jsonPath = extractable.full_path
        except AttributeError:
            jsonPath = None

        for e in extracted_results:
            # for the purpose of provenance hierarrchy tracking, a parent's id for next generation.
            e.prov_id = self.provenance_id_index
            extraction_provenance_record: ExtractionProvenanceRecord = ExtractionProvenanceRecord(
                e.prov_id, jsonPath, e.provenance["extractor_name"],
                e.provenance["start_char"], e.provenance["end_char"], e.provenance["confidence"], self,
                extractable.prov_id)
            self._provenances[e.prov_id] = extraction_provenance_record

            # for the purpose of provenance hierarchy tracking
            self.provenance_id_index_incrementer()
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

    def create_provenance(self, extraction_provenance_record: ExtractionProvenanceRecord) -> None:
        if "provenances" not in self.cdr_document:
            self.cdr_document["provenances"] = []
        self.cdr_document["provenances"].append(self.get_dict_extraction_provenance(extraction_provenance_record))

    def get_dict_extraction_provenance(self, extraction_provenance_record: ExtractionProvenanceRecord):
        prov_dict = dict()
        prov_dict["@id"] = extraction_provenance_record.id
        prov_dict["@type"] = "extraction_provenance_record"
        prov_dict["method"] = extraction_provenance_record.method
        prov_dict["confidence"] = extraction_provenance_record.extraction_confidence
        if extraction_provenance_record.origin_record.full_path is not None:
            jsonpath = extraction_provenance_record.origin_record.full_path
            if jsonpath in self._jsonpath_provenances:
                self._jsonpath_provenances[jsonpath].append(extraction_provenance_record.id)
            else:
                self._jsonpath_provenances[jsonpath] = [extraction_provenance_record.id]
            origin_dict = dict()
            origin_dict["path"] = jsonpath
            origin_dict["start_char"] = extraction_provenance_record.origin_record.start_char
            origin_dict["end_char"] = extraction_provenance_record.origin_record.end_char
            prov_dict["origin_record"] = origin_dict
        if extraction_provenance_record.parent_extraction_provenance is not None:
            prov_dict["parent_provenance_id"] = extraction_provenance_record.parent_extraction_provenance
        return prov_dict

    def insert_kg_into_cdr(self):
        if self.kg and self.kg.value:
            self.cdr_document["knowledge_graph"] = self.kg.value

    def build_knowledge_graph(self, json_ontology: dict) -> List:
        """
        The idea if to be able to build a json knowledge graph from a json like ontology representation, eg:
         kg_object_ontology = {
            "uri": doc.doc_id,
            "country": doc.select_segments("$.Location"),
            "type": [
                "Event",
                doc.extract(self.incomp_decoder, doc.select_segments("$.Incomp")[0]),
                doc.extract(self.int_decoder, doc.select_segments("$.Int")[0])
            ]
        }
        Currently json ontology representation is supported, might add new ways later
        Args:
            json_ontology: a json ontology representation of a knowledge graph

        Returns: returns  a list of nested documents created

        """
        nested_docs = list()
        if json_ontology:
            for key in list(json_ontology):
                j_values = json_ontology[key]
                if not isinstance(j_values, list):
                    j_values = [j_values]
                for j_value in j_values:
                    if not isinstance(j_value, dict):
                        if self.kg:
                            if key not in ['doc_id', 'uri']:
                                self.kg.add_value(key, value=j_value)
                    else:
                        """Now we have to create a nested document, assign it a doc_id and 
                           add the doc_id to parent document's knowledge graph"""
                        child_doc_id = None
                        if 'uri' in j_value:
                            child_doc_id = j_value['uri']
                        elif 'doc_id' in j_value:
                            child_doc_id = j_value['doc_id']

                        child_doc = Document(self.etk, cdr_document=dict(), mime_type='json', url='')
                        nested_docs.extend(child_doc.build_knowledge_graph(j_value))

                        if not child_doc_id:
                            child_doc_id = Utility.create_doc_id_from_json(child_doc.kg._kg)

                        if self.kg:
                            self.kg.add_value(key, value=child_doc_id)
                        child_doc.cdr_document["doc_id"] = child_doc_id

                        nested_docs.append(child_doc)

        return nested_docs

    def add_type(self, type_):
        self.kg.add_value('@type', value=type_)

    def with_type(self, type_):
        if type_:
            self.add_type(type_)
        return self
