import unittest, json

from etk.document_selector import DefaultDocumentSelector
from etk.etk import ETK
from etk.knowledge_graph_schema import KGSchema

sample_input = {
    "dataset": "unittestUnittestUNITTEST",
    "website": "abcABCAbc",
    "url": "zxcZXCZxc",
    "projects": [
        {
            "name": "etk",
            "description": "version 2 of etk, implemented by Runqi, Dongyu, Sylvia, Amandeep and others."
        },
        {
            "name": "rltk",
            "description": "record linkage toolkit, implemented by Pedro, Mayank, Yixiang and several students."
        }
    ]
}

kg_schema = KGSchema(json.load(open('etk/unit_tests/ground_truth/test_config.json')))
etk = ETK(kg_schema=kg_schema)

class TestDocumentSelector(unittest.TestCase):

    def test_datasets_condition(self) -> None:
        doc = etk.create_document(sample_input)
        default_doc_selector = DefaultDocumentSelector()
        res_true = default_doc_selector.select_document(doc, datasets=[".*unittest", ".*abc"])
        res_false = default_doc_selector.select_document(doc, datasets=[".*abc", ".*hhhh"])
        self.assertEqual(True, res_true)
        self.assertEqual(False, res_false)

    def test_url_patterns_condition(self) -> None:
        doc = etk.create_document(sample_input)
        default_doc_selector = DefaultDocumentSelector()
        res_true = default_doc_selector.select_document(doc, url_patterns=[".*unittest", ".*zxc"])
        res_false = default_doc_selector.select_document(doc, url_patterns=[".*ZXc", ".*hhhh"])
        self.assertEqual(True, res_true)
        self.assertEqual(False, res_false)

    def test_website_patterns_condition(self) -> None:
        doc = etk.create_document(sample_input)
        default_doc_selector = DefaultDocumentSelector()
        res_true = default_doc_selector.select_document(doc, website_patterns=[".*unittest", ".*abc"])
        res_false = default_doc_selector.select_document(doc, website_patterns=[".*ABc", ".*hhhh"])
        self.assertEqual(True, res_true)
        self.assertEqual(False, res_false)

    def test_json_paths_and_json_paths_regex(self) -> None:
        doc = etk.create_document(sample_input)
        default_doc_selector = DefaultDocumentSelector()
        res_true = default_doc_selector.select_document(doc, json_paths=["$.website"],
                                                        json_paths_regex=[".*unittest", ".*abc"])
        res_false = default_doc_selector.select_document(doc, json_paths=["$.website"], json_paths_regex=[".*hhhh"])
        self.assertEqual(True, res_true)
        self.assertEqual(False, res_false)

    def test_all_condition(self) -> None:
        doc = etk.create_document(sample_input)
        default_doc_selector = DefaultDocumentSelector()
        res_true = default_doc_selector.select_document(doc, datasets=[".*unittest", ".*abc"],
                                                        url_patterns=[".*unittest", ".*zxc"],
                                                        website_patterns=[".*unittest", ".*abc"],
                                                        json_paths=["$.website"],
                                                        json_paths_regex=[".*unittest", ".*abc"])
        res_false = default_doc_selector.select_document(doc, datasets=[".*abc", ".*hhhh"],
                                                         url_patterns=[".*ZXc", ".*hhhh"],
                                                         website_patterns=[".*ABc", ".*hhhh"], json_paths=["$.website"],
                                                         json_paths_regex=[".*hhhh"])
        self.assertEqual(True, res_true)
        self.assertEqual(False, res_false)


if __name__ == '__main__':
    unittest.main()
