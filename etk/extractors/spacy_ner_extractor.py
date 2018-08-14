import spacy
from etk.extractor import Extractor, InputType
from etk.extraction import Extraction
from typing import List


class SpacyNerExtractor(Extractor):
    def __init__(self, extractor_name: str, nlp=spacy.load('en_core_web_sm')):
        Extractor.__init__(self, input_type=InputType.TEXT,
                           category="built_in_extractor",
                           name=extractor_name)
        self.nlp = nlp

    # all_attrs = ['PERSON', 'NORP', 'FAC', 'ORG', 'GPE', 'LOC', 'PRODUCT', 'EVENT', 'WORK_OF_ART', 'LAW', 'LANGUAGE',
    #              'DATE', 'TIME', 'PERCENT', 'MONEY', 'QUANTITY', 'ORDINAL', 'CARDINAL']
    def extract(self, text: str, get_attr=['PERSON', 'ORG', 'GPE']) -> List[Extraction]:
        doc = self.nlp(text)
        attr_list = list()
        for ent in doc.ents:
            if ent.label_ in get_attr:
                attr_list.append(Extraction(extractor_name=self.name,
                                            start_char=int(ent.start_char),
                                            end_char=int(ent.end_char),
                                            value=ent.text,
                                            tag=ent.label_))
        return attr_list
