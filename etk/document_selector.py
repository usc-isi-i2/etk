from typing import List
import re
from jsonpath_ng import jsonpath, parse
from etk.document import Document


class DocumentSelector(object):
    """
    Abstract class to encapsulate logic to enable ETK to selectively apply different extractors to different
    documents. Enables users of ETK to write custom logic for selecting documents for processing.
    """

    def select_document(self, document: Document) -> bool:
        """
        Args:
            document (Document): the document to be tested

        Returns: True, if the document should be selected for processing

        """


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

    # re1 = r'\d+\.\d*[L][-]\d*\s[A-Z]*[/]\d*'
    # re2 = '\d*[/]\d*[A-Z]*\d*\s[A-Z]*\d*[A-Z]*'
    # re3 = '[A-Z]*\d+[/]\d+[A-Z]\d+'
    # re4 = '\d+[/]\d+[A-Z]*\d+\s\d+[A-z]\s[A-Z]*'

    # sentences = [string1, string2, string3, string4]
    # generic_re = re.compile("(%s|%s|%s|%s)" % (re1, re2, re3, re4)).findall(sentence)

    def select_document(self,
                        document: Document,
                        datasets: List[str] = None,
                        url_patterns: List[str] = None,
                        website_patterns: List[str] = None,
                        json_paths: List[str] = None,
                        json_paths_regex: List[str] = None) -> bool:

        json_doc = document.cdr_document

        if (json_paths_regex is not None) and (json_paths is None):
            print("please specify both json_paths_regex and json_paths")
            # TODO: print out some error message here
            return False

        if datasets is not None:
            res = self.check_content(json_doc, "$.dataset", datasets, False)
            if not res:
                return False

        if url_patterns is not None:
            res = self.check_content(json_doc, "$.url", url_patterns, True)
            if not res:
                return False

        if website_patterns is not None:
            res = self.check_content(json_doc, "$.website", website_patterns, True)
            if not res:
                return False

        if json_paths is not None:
            rw_json_paths = []
            for json_path in json_paths:
                rw_json_paths.append(parse(json_path))

        if json_paths_regex is not None:
            rw_json_paths_regex = re.compile('|'.join(json_paths_regex))
            # TODO AMandeep: what is going at the line below
            res = self.check_json_path_codition(json_doc, rw_json_paths, rw_json_paths_regex)
            if not res:
                return False

        return True

    @staticmethod
    def check_content(json_doc: dict, json_path: str, patterns: List[str], enable_regexp: bool = True) -> bool:
        if enable_regexp:
            # compile json path
            patterns = re.compile('|'.join(patterns))
        else:
            patterns = '|'.join(patterns)

        rw_jsonpath = parse(json_path)

        # find all matches value
        values = [match.value for match in rw_jsonpath.find(json_doc)]
        # test if any matched value satisfies condition
        res = any(re.search(patterns, value) != None for value in values)

        return res

    @staticmethod
    def check_json_path_codition(json_doc: dict,
                                 rw_json_paths: List[jsonpath.Child],
                                 compiled_json_paths_regex: str) -> bool:
        for json_path_expr in rw_json_paths:
            values = [match.value for match in json_path_expr.find(json_doc)]
            res = any(re.search(compiled_json_paths_regex, value) is not None for value in values)
            if res:
                return True

        return False
