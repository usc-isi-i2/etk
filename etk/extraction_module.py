from etk.document import Document
from etk.document_selector import DefaultDocumentSelector
from typing import List
from etk.knowledge_graph import KnowledgeGraph


class ExtractionModule(object):
    """
    Abstract class for extraction module
    """
    def __init__(self, etk):
        self.etk = etk

    def process_document(self, doc: Document, kg: KnowledgeGraph):
        """
        Add your code for processing the document
        """
        pass

    def document_selector(self, doc: Document) -> bool:
        """
        Boolean function for selecting document
        Args:
            doc: Document

        Returns:

        """
        return DefaultDocumentSelector().select_document(doc)

    @property
    def produces(self) -> List[str]:
        return []

    @property
    def require(self) -> List[str]:
        return []
