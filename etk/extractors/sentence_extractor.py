from typing import List
from etk.extraction import Extraction
from etk.extractor import Extractor, InputType

import copy
import spacy


class SentenceExtractor(Extractor):
    """
    Extract individual sentences using lightweight spaCy module.
    """

    def __init__(self, name: str = None, custom_nlp: type = None) -> None:
        Extractor.__init__(self,
                           input_type=InputType.TEXT,
                           category="Text extractor",
                           name=name if name else "Sentence extractor")

        load_parser = False
        if custom_nlp:
            try:
                custom_pipeline = copy.deepcopy(custom_nlp)
                pipe_names = custom_pipeline.pipe_names
                for pipe in pipe_names:
                    if pipe != "parser":
                        custom_pipeline.remove_pipe(pipe)

                try:
                    assert "parser" in custom_pipeline.pipe_names
                    self.parser = custom_pipeline
                except AssertionError:
                    print("Note: custom_pipeline does not have a parser. \n"
                          "Loading parser from en_core_web_sm... ")
                    load_parser = True

            except AttributeError as e:
                print("Note: custom_pipeline does not have expected "
                      "attributes.")
                print(e)
                print("Loading parser from en_core_web_sm...")
                load_parser = True
        else:
            load_parser = True

        if load_parser:
            self.parser = spacy.load("en_core_web_sm",
                                     disable=["tagger", "ner"])

    def extract(self, text: str, docId=None) -> List[Extraction]:
        """
        Splits text by sentences.

        Args:
            text (str): Input text to be extracted.

        Returns:
            List[Extraction]: the list of extraction or the empty list if there are no matches.
        """

        doc = self.parser(text)

        extractions = list()
        sentence_count = 1
        for sent in doc.sents:
            if docId:
                faiss_id = docId + '{:>04d}'.format(sentence_count)
                assert len(faiss_id) ==19, "Faiss ID length is not correct"
                val = (faiss_id, sent.text)
            else:
                val = sent.text
            this_extraction = Extraction(value=val,
                                         extractor_name=self.name,
                                         start_token=sent[0],
                                         end_token=sent[-1],
                                         start_char=sent.text[0],
                                         end_char=sent.text[-1])
            sentence_count += 1
            print(this_extraction)
            extractions.append(this_extraction)
        print("Done with " + docId)
        return extractions
