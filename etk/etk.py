import platform
import tempfile
from typing import List, Dict
import spacy, copy, json, os, jsonpath_ng, importlib, logging, sys
from etk.tokenizer import Tokenizer
from etk.document import Document
from etk.etk_exceptions import InvalidJsonPathError
from etk.etk_module import ETKModule
from etk.etk_exceptions import ErrorPolicy, NotGetETKModuleError
from etk.utilities import Utility
import gzip

TEMP_DIR = '/tmp' if platform.system() == 'Darwin' else tempfile.gettempdir()


class ETK(object):
    def __init__(self, kg_schema=None, modules=None, extract_error_policy="process", logger=None,
                 logger_path=os.path.join(TEMP_DIR, 'etk.log'), ontology=None, generate_json_ld=False):

        self.generate_json_ld = generate_json_ld

        if logger:
            self.logger = logger
        else:
            logging.basicConfig(
                level=logging.DEBUG,
                format='%(asctime)s %(name)-6s %(levelname)s %(message)s',
                datefmt='%m-%d %H:%M',
                filename=logger_path,
                filemode='w'
            )
            self.logger = logging.getLogger('ETK')

        self.parser = jsonpath_ng.parse
        self.default_nlp = spacy.load('en_core_web_sm')
        self.default_tokenizer = Tokenizer(copy.deepcopy(self.default_nlp))
        self.parsed = dict()
        self.kg_schema = kg_schema
        self.ontology = ontology
        self.em_lst = list()
        if modules:
            if isinstance(modules, list):
                for module in modules:
                    if isinstance(module, str):
                        self.em_lst.extend(self.load_ems(modules))
                    elif issubclass(module, ETKModule):
                        self.em_lst.append(module(self))
            elif issubclass(modules, ETKModule):
                self.em_lst = [modules(self)]
            else:
                raise NotGetETKModuleError("Not getting extraction module")

        if extract_error_policy.lower() == "throw_extraction":
            self.error_policy = ErrorPolicy.THROW_EXTRACTION
        if extract_error_policy.lower() == "throw_document":
            self.error_policy = ErrorPolicy.THROW_DOCUMENT
        if extract_error_policy.lower() == "raise_error":
            self.error_policy = ErrorPolicy.RAISE
        else:
            self.error_policy = ErrorPolicy.PROCESS

    def create_document(self, doc: Dict, mime_type: str = None, url: str = "http://ex.com/123",
                        doc_id=None, type_=None) -> Document:
        """
        Factory method to wrap input JSON docs in an ETK Document object.

        Args:
            doc (object): a JSON object containing a document in CDR format.
            mime_type (str): if doc is a string, the mime_type tells what it is
            url (str): if the doc came from the web, specifies the URL for it
            doc_id
            type_

        Returns: wrapped Document

        """
        return Document(self, doc, mime_type, url, doc_id=doc_id).with_type(type_)

    def parse_json_path(self, jsonpath):

        """
        Parse a jsonpath

        Args:
            jsonpath: str

        Returns: a parsed json path

        """
        if jsonpath not in self.parsed:
            try:
                self.parsed[jsonpath] = self.parser(jsonpath)
            except Exception:
                self.log("Invalid Json Path: " + jsonpath, "error")
                raise InvalidJsonPathError("Invalid Json Path")

        return self.parsed[jsonpath]

    def process_and_frame(self, doc: Document):
        """
        Processes a document and if it has child docs, embeds them in the parent document. Only works for 1 level of
        nesting. Kind of hack, will implement properly later
        Args:
            doc: input document to be run etk modules on

        Returns:

        """
        nested_docs = self.process_ems(doc)
        parent_kg = doc.cdr_document.get('knowledge_graph', None)
        if parent_kg:
            if nested_docs and len(nested_docs) > 0:
                for nested_doc in nested_docs:
                    json_doc = nested_doc.cdr_document
                    doc_id = json_doc['doc_id']
                    if doc_id != doc.doc_id:
                        for field_name in list(parent_kg):
                            field_extractions = parent_kg[field_name]
                            if not isinstance(field_extractions, list):
                                field_extractions = [field_extractions]
                            for i in range(0, len(field_extractions)):
                                field_extraction = field_extractions[i]
                                if 'value' in field_extraction and field_extraction['value'] == doc_id:
                                    del field_extractions[i]
                                    field_extractions.append(
                                        {'value': json_doc, 'key': field_extraction['key'], 'is_nested': True})

    def process_ems(self, doc: Document) -> List[Document]:
        """
        Factory method to wrap input JSON docs in an ETK Document object.

        Args:
            doc (Document): process on this document

        Returns: a Document object and a KnowledgeGraph object

        """
        new_docs = list()

        for a_em in self.em_lst:
            if a_em.document_selector(doc):
                self.log(" processing with " + str(type(a_em)) + ". Process", "info", doc.doc_id, doc.url)
                fresh_docs = a_em.process_document(doc)
                # Allow ETKModules to return nothing in lieu of an empty list (people forget to return empty list)
                if fresh_docs:
                    new_docs.extend(fresh_docs)
            # try:
            #     if a_em.document_selector(doc):
            #         self.log(" processing with " + str(type(a_em)) + ". Process", "info", doc.doc_id, doc.url)
            #         new_docs.extend(a_em.process_document(doc))
            # except Exception as e:
            #     if self.error_policy == ErrorPolicy.THROW_EXTRACTION:
            #         self.log(str(e) + " processing with " + str(type(a_em)) + ". Continue", "error", doc.doc_id,
            #                  doc.url)
            #         continue
            #     if self.error_policy == ErrorPolicy.THROW_DOCUMENT:
            #         self.log(str(e) + " processing with " + str(type(a_em)) + ". Throw doc", "error", doc.doc_id,
            #                  doc.url)
            #         return list()
            #     if self.error_policy == ErrorPolicy.RAISE:
            #         self.log(str(e) + " processing with " + str(type(a_em)), "error", doc.doc_id, doc.url)
            #         raise e

        # Do house cleaning.
        doc.insert_kg_into_cdr()
        if not self.generate_json_ld:
            if "knowledge_graph" in doc.cdr_document:
                doc.cdr_document["knowledge_graph"].pop("@context", None)
        Utility.make_json_serializable(doc.cdr_document)
        if not doc.doc_id:
            doc.doc_id = Utility.create_doc_id_from_json(doc.cdr_document)

        results = [doc]
        for new_doc in new_docs:
            results.extend(self.process_ems(new_doc))

        return results

    @staticmethod
    def load_glossary(file_path: str, read_json=False) -> List[str]:
        """
        A glossary is a text file, one entry per line.

        Args:
            file_path (str): path to a text file containing a glossary.
            read_json (bool): set True if the glossary is in json format
        Returns: List of the strings in the glossary.
        """
        if read_json:
            if file_path.endswith(".gz"):
                return json.load(gzip.open(file_path))
            return json.load(open(file_path))

        return open(file_path).read().splitlines()

    @staticmethod
    def load_spacy_rule(file_path: str) -> Dict:
        """
        A spacy rule file is a json file.

        Args:
            file_path (str): path to a text file containing a spacy rule sets.

        Returns: Dict as the representation of spacy rules
        """
        with open(file_path) as fp:
            return json.load(fp)

    def load_ems(self, modules_paths: List[str]):
        """
        Load all extraction modules from the path

        Args:
            modules_path: str

        Returns:

        """
        all_em_lst = []
        if modules_paths:
            for modules_path in modules_paths:
                em_lst = []
                try:
                    for file_name in os.listdir(modules_path):
                        if file_name.startswith("em_") and file_name.endswith(".py"):
                            sys.path.append(modules_path)  # append module dir path
                            this_module = importlib.import_module(file_name[:-3])
                            for em in self.classes_in_module(this_module):
                                em_lst.append(em(self))
                except:
                    self.log("Error when loading etk modules from " + modules_path, "error")
                    raise NotGetETKModuleError("Wrong file path for ETK modules")
                all_em_lst += em_lst

        try:
            all_em_lst = self.topological_sort(all_em_lst)
        except Exception:
            self.log("Topological sort for ETK modules fails", "error")
            raise NotGetETKModuleError("Topological sort for ETK modules fails")

        # if not all_em_lst:
        #     self.log("No ETK module in " + str(modules_paths), "error")
        #     raise NotGetETKModuleError("No ETK module in dir, module file should start with em_, end with .py")
        return all_em_lst

    @staticmethod
    def topological_sort(etk_module_list: List[ETKModule]) -> List[ETKModule]:
        """
        Return topological order of ems

        Args:
            lst: List[ExtractionModule]

        Returns: List[ExtractionModule]

        """
        "TODO"
        return etk_module_list

    @staticmethod
    def classes_in_module(module) -> List:
        """
        Return all classes with super class ExtractionModule

        Args:
            module:

        Returns: List of classes

        """
        md = module.__dict__
        return [
            md[c] for c in md if (
                    isinstance(md[c], type) and
                    issubclass(md[c], ETKModule
                               ) and
                    md[c].__module__ == module.__name__)
        ]

    def log(self, message, level, doc_id=None, url=None):
        message = message + " doc_id: {}".format(doc_id) + " url: {}".format(url)

        if level == "error":
            self.logger.error(message)
        elif level == "warning":
            self.logger.warning(message)
        elif level == "info":
            self.logger.info(message)
        elif level == "debug":
            self.logger.debug(message)
        elif level == "critical":
            self.logger.critical(message)
        elif level == "exception":
            self.logger.exception(message)
