from typing import List
from etk.extraction import Extraction
from etk.extractor import Extractor, InputType

import copy
import spacy


class SentenceExtractor(Extractor):
    """
    **Description**
        Extract individual sentences using lightweight spaCy module.

    Example:
        ::

            sentence_extractor = SentenceExtractor(custom_nlp=nlp)
            sentence_extractor.extract(text=text)
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
                    self._parser = custom_pipeline
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
            self._parser = spacy.load("en_core_web_sm",
                                     disable=["tagger", "ner"])

    def extract(self, text: str) -> List[Extraction]:
        """
        Splits text by sentences.

        Args:
            text (str): Input text to be extracted.

        Returns:
            List[Extraction]: the list of extraction or the empty list if there are no matches.
        """

        doc = self._parser(text)

        extractions = list()
        for sent in doc.sents:
            this_extraction = Extraction(value=sent.text,
                                         extractor_name=self.name,
                                         start_token=sent[0],
                                         end_token=sent[-1],
                                         start_char=sent.text[0],
                                         end_char=sent.text[-1])
            extractions.append(this_extraction)

        return extractions
