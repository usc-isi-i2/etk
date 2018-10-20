import spacy
from etk.extractor import Extractor, InputType
from etk.extraction import Extraction
from typing import List


class SpacyNerExtractor(Extractor):
    """
    **Description**
        This extractor takes a list of spaCy NER tag as reference, and extract
        the tag matched substring from the input text

    Examples:
        ::

            get_attr = ['PERSON', 'ORG', 'GPE']
            spacy_ner_extractor = SpacyNerExtractor()
            spacy_ner_extractor.extract(text=text, get_attr=get_attr)

    """
    def __init__(self, extractor_name: str, nlp=spacy.load('en_core_web_sm')):
        Extractor.__init__(self, input_type=InputType.TEXT,
                           category="built_in_extractor",
                           name=extractor_name)
        self.__nlp = nlp

    # all_attrs = ['PERSON', 'NORP', 'FAC', 'ORG', 'GPE', 'LOC', 'PRODUCT', 'EVENT', 'WORK_OF_ART', 'LAW', 'LANGUAGE',
    #              'DATE', 'TIME', 'PERCENT', 'MONEY', 'QUANTITY', 'ORDINAL', 'CARDINAL']
    def extract(self, text: str, get_attr=['PERSON', 'ORG', 'GPE']) -> List[Extraction]:
        """
        Args:
            text (str): the text to extract from.
            get_attr (List[str]): The spaCy NER attributes we're interested in.

        Returns:
            List(Extraction): the list of extraction or the empty list if there are no matches.
        """
        doc = self.__nlp(text)
        attr_list = list()
        for ent in doc.ents:
            if ent.label_ in get_attr:
                attr_list.append(Extraction(extractor_name=self.name,
                                            start_char=int(ent.start_char),
                                            end_char=int(ent.end_char),
                                            value=ent.text,
                                            tag=ent.label_,
                                            start_token=ent.start,
                                            end_token=ent.end))
        return attr_list
