import os
import sys
import json
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from etk.etk import ETK
from etk.document import Document
from etk.etk_module import ETKModule
from etk.extractors.sentence_extractor import SentenceExtractor


class SentenceSplittingETKModule(ETKModule):
    """
    Abstract class for extraction module
    """

    def __init__(self, etk: ETK):
        ETKModule.__init__(self, etk)
        self.sentence_extractor = SentenceExtractor(name="My sentence splitter")

    def process_document(self, doc: Document):
        """
        Add your code for processing the document
        """

        text_to_be_split = doc.select_segments("doc_text[*].text")
        units_of_text = doc.select_segments("doc_text[*]")

        for t, u in zip(text_to_be_split, units_of_text):
            split_sentences = doc.extract(self.sentence_extractor, t)
            u.store(split_sentences, "split_sentences")

        return list()


if __name__ == "__main__":

    toy_doc = {
        "doc_text": [
            {
                "name": "Control - Single Sentence",
                "description": "This sentence should not be split",
                "text": "How much wood could a woodchuck chuck if a woodchuck "
                        "had long arms?"
            },
            {
                "name": "Control - Conversational Text",
                "description": "These sentences should split into three parts",
                "text": "The fact of the matter is this: nobody has ever seen "
                        "a woodchuck go super-sayan. Even if it were possible, "
                        "the likelihood that they would actually throw a piece "
                        "of wood is preposterously low. It's just not going to happen!"
            },
            {
                "name": "Test1 - Butchered Conversational Text",
                "description": "Modified sentence boundary conditions",
                "text": "The fact of the matter is this: Nobody has ever seen "
                        "a woodchuck go super-sayan.even if it were possible, "
                        "the likelihood that they would actually throw a piece "
                        "of wood is preposterously low. it's just not going to happen!"
            },
            {
                "name": "Test2 - Butchered Wikipedia Article",
                "description": "Similarly modified sentence boundary conditions, "
                               "but this text is more regular in structure",
                "text": "The groundhog (Marmota monax), also known as a woodchuck, "
                        "is a rodent of the family Sciuridae, belonging to the group "
                        "of large ground squirrels known as marmots.It was first "
                        "scientifically described by Carl Linnaeus in 1758...the "
                        "groundhog is also referred to as a chuck, wood-shock, "
                        "groundpig, whistlepig, whistler, thickwood badger, "
                        "Canada marmot, monax, moonack, weenusk, red monk and, "
                        "among French Canadians in eastern Canada, siffleur"
            },
            {
                "name": "Test3 - Social Media",
                "description": "Parser stress test for tweets",
                "text": "Slides onto twitter..... \n"
                        ".......slippery floor....... \n"
                        "............slides out the other side..."
            }
        ],
        "doc_id": 42069
    }

    etk = ETK(modules=SentenceSplittingETKModule)
    doc = etk.create_document(toy_doc)

    split_doc = etk.process_ems(doc)

    print(json.dumps(split_doc[0].value, indent=2))
