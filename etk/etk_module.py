from etk.document import Document
from etk.document_selector import DefaultDocumentSelector
from typing import List


class ETKModule(object):
    """
    Abstract class for extraction module
    """

    def __init__(self, etk):
        self.etk = etk

    def process_document(self, doc: Document) -> List[Document]:
        """
        Add your code for processing the document

        Returns:
            A list of new Document(s) created in this function, otherwise empty list()
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
