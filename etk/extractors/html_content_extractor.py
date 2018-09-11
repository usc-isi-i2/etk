from typing import List
from enum import Enum, auto
from bs4 import BeautifulSoup
from bs4.element import Comment
from etk.extractor import Extractor, InputType
from etk.extraction import Extraction
from etk.extractors.readability.readability import Document


class Strategy(Enum):
    """
    ALL_TEXT: return all visible text in an HTML page
    MAIN_CONTENT_STRICT: MAIN_CONTENT_STRICT: return the main content of the page without boiler plate (menu, ads...)
    MAIN_CONTENT_RELAXED: variant of MAIN_CONTENT_STRICT with less strict rules
    """
    ALL_TEXT = auto()
    MAIN_CONTENT_STRICT = auto()
    MAIN_CONTENT_RELAXED = auto()


class HTMLContentExtractor(Extractor):
    """
    **Description**
        This class extracts text from HTML pages. Uses readability and BeautifulSoup.

    Examples:
        ::

            html_content_extractor = HTMLContentExtractor()
            html_content_extractor.extract(text=input_doc,
                                        strategy=Strategy.ALL_TEXT)

    """

    def __init__(self):
        Extractor.__init__(self,
                           input_type=InputType.HTML,
                           category="HTML extractor",
                           name="HTML content extractor")

    def extract(self, html_text: str, strategy: Strategy=Strategy.ALL_TEXT) \
            -> List[Extraction]:
        """
        Extracts text from an HTML page using a variety of strategies

        Args:
            html_text (str): html page in string
            strategy (enum[Strategy.ALL_TEXT, Strategy.MAIN_CONTENT_RELAXED, Strategy.MAIN_CONTENT_STRICT]): one of
            Strategy.ALL_TEXT, Strategy.MAIN_CONTENT_STRICT and Strategy.MAIN_CONTENT_RELAXED

        Returns:
             List[Extraction]: typically a singleton list with the extracted text
        """

        if html_text:
            if strategy == Strategy.ALL_TEXT:
                soup = BeautifulSoup(html_text, 'html.parser')
                texts = soup.findAll(text=True)
                visible_texts = filter(self._tag_visible, texts)
                all_text = u" ".join(t.strip() for t in visible_texts)
                return [Extraction(all_text, self.name)]
            else:
                relax = strategy == Strategy.MAIN_CONTENT_RELAXED
                readable = Document(html_text, recallPriority=relax).summary(html_partial=False)
                clean_text = BeautifulSoup(readable.encode('utf-8'), 'lxml').strings
                readability_text = ' '.join(clean_text)
                return [Extraction(readability_text, self.name)]
        else:
            return []

    @staticmethod
    def _tag_visible(element):
        if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
            return False
        if isinstance(element, Comment):
            return False
        return True

