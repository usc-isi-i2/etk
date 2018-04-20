from typing import List, Dict
import spacy, copy, json, os, jsonpath_ng, importlib, logging
from etk.tokenizer import Tokenizer
from etk.document import Document
from etk.etk_exceptions import InvalidJsonPathError
from etk.etk_module import ETKModule
from etk.etk_exceptions import ErrorPolicy, NotGetETKModuleError


class ETK(object):
    def __init__(self, kg_schema=None, modules=None, extract_error_policy="process", logger=None,
                 logger_path='/tmp/etk.log'):
        self.parser = jsonpath_ng.parse
        self.default_nlp = spacy.load('en_core_web_sm')
        self.default_tokenizer = Tokenizer(copy.deepcopy(self.default_nlp))
        self.parsed = dict()
        self.kg_schema = kg_schema
        if modules:
            if type(modules) == list:
                self.em_lst = self.load_ems(modules)
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

    def create_document(self, doc: Dict, mime_type: str = None, url: str = "http://ex.com/123",
                        doc_id=None) -> Document:
        """
        Factory method to wrap input JSON docs in an ETK Document object.

        Args:
            doc (object): a JSON object containing a document in CDR format.
            mime_type (str): if doc is a string, the mime_type tells what it is
            url (str): if the doc came from the web, specifies the URL for it
            doc_id

        Returns: wrapped Document

        """
        return Document(self, doc, mime_type, url, doc_id=doc_id)

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

    def process_ems(self, doc: Document):
        """
        Factory method to wrap input JSON docs in an ETK Document object.

        Args:
            doc (Document): process on this document

        Returns: a Document object and a KnowledgeGraph object

        """
        for a_em in self.em_lst:
            try:
                if a_em.document_selector(doc):
                    self.log(" processing with " + str(type(a_em)) + ". Process", "info", doc.doc_id, doc.url)
                    a_em.process_document(doc)
            except Exception as e:
                if self.error_policy == ErrorPolicy.THROW_EXTRACTION:
                    self.log(str(e) + " processing with " + str(type(a_em)) + ". Continue", "error", doc.doc_id,
                             doc.url)
                    continue
                if self.error_policy == ErrorPolicy.THROW_DOCUMENT:
                    self.log(str(e) + " processing with " + str(type(a_em)) + ". Throw doc", "error", doc.doc_id,
                             doc.url)
                    return None
                if self.error_policy == ErrorPolicy.RAISE:
                    self.log(str(e) + " processing with " + str(type(a_em)), "error", doc.doc_id, doc.url)
                    raise e

        doc.insert_kg_into_cdr()
        return doc, doc.kg

    @staticmethod
    def load_glossary(file_path: str, read_json=False) -> List[str]:
        """
        A glossary is a text file, one entry per line.

        Args:
            file_path (str): path to a text file containing a glossary.
            read_json (bool): set True if the glossary is in json format
        Returns: List of the strings in the glossary.
        """
        with open(file_path) as fp:
            if read_json:
                return json.load(fp)
            return fp.read().splitlines()

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
                modules_path = modules_path.strip(".").strip("/")
                try:
                    for file_name in os.listdir(modules_path):
                        if file_name.startswith("em_") and file_name.endswith(".py"):
                            this_module = importlib.import_module(modules_path + "." + file_name[:-3])
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

        if not all_em_lst:
            self.log("No ETK module in " + str(modules_paths), "error")
            raise NotGetETKModuleError("No ETK module in dir, module file should start with em_, end with .py")
        return all_em_lst


    @staticmethod
    def topological_sort(lst: List[ETKModule]) -> List[ETKModule]:
        """
        Return topological order of ems

        Args:
            lst: List[ExtractionModule]

        Returns: List[ExtractionModule]

        """
        "TODO"
        return lst

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
