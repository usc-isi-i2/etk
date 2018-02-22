import json
import jsonpath_rw
import re
from document import Document


class DefaultDocumentSelector(DocumentSelector):

    def __init__(self):
        pass
    """
    A concrete implementation of DocumentSelector that supports commonly used methods for
    selecting documents.
    """

    """
        Args:
            document ():
            datasets (List[str]): test the "dataset" attribute in the doc contains any of the strings provided
            url_patterns (List[str]): test the "url" of the doc matches any of the regexes using regex.search
            website_patterns (List[str]): test the "website" of the doc contains any of the regexes using regex.search
            json_paths (List[str]): test existence of any of the given JSONPaths in a document
            json_paths_regex(List[str]): test that any of the values selected in 'json_paths' satisfy any of
              the regexex provided using regex.search

        Returns: True if the document satisfies all non None arguments.
        Each of the arguments can be a list, in which case satisfaction means
        that one of the elements in the list is satisfied, i.e., it is an AND of ORs

        For efficiency, the class caches compiled representations of all regexes and json_paths given
        that the same selectors will be used for consuming all documents in a stream.
        """
    def select_document(self,
                        document: Document,
                        datasets: List[str] = None,
                        url_patterns: List[str] = None,
                        website_patterns: List[str] = None,
                        json_paths: List[str] = None,
                        json_paths_regex: List[str] = None) -> bool:

        if (json_paths_regex is not None) and (json_paths is None):
            # TODO: print out some error message here

        if url_patterns is not None:
            rw_url_patterns = map(lambda re: re.compile(), url_patterns)

        if website_patterns is not None:
            rw_website_patterns = map(lambda re: re.compile(), website_patterns)

        if json_paths is not None:
            rw_json_paths = map(lambda jsonpath_rw: jsonpath_rw.parse(), json_paths)

        if json_paths_regex is not None:
            rw_json_paths_regex = map(lambda re: re.compile(), json_paths_regex)


        doc = Document.cdr_document

        return False;

        raise NotImplementedError

    def check_datasets_condition(self, json_doc: dict, datasets: List[str]) -> bool:

        raise NotImplementedError

    def check_url_patterns_condition(self, json_doc: dict, 
                                    compiled_url_patterns: List[str]) -> bool:

        raise NotImplementedError

    def check_website_patterns_condition(self, json_doc: dict, 
                                        compiled_website_patterns: List[str]) -> bool:
        
        raise NotImplementedError

    def check_json_path_codition(self, json_doc: dict, 
                                rw_json_paths: List[jsonpath_rw.jsonpath.Child], 
                                compiled_json_paths_regex: List[str]) -> bool:
        for json_path_expr in rw_json_paths:
            json_path_values = [match.value for match in json_path_expr.find(json_doc)]
            for 

            pass
        raise NotImplementedError









