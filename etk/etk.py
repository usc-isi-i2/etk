from typing import List
import json


class ETK(object):

    def __init__(self):
        pass

    def load_glossary(self, file_path) -> List[str]:
        """
        A glossary is a text file, one entry per line.

        Args:
            file_path (str): path to a text file containing a glossary.

        Returns: List of the strings in the glossary.
        """
        #to-do: this should be a list, not a dict
        res = dict()
        with open(file_path) as fp:
            line = fp.readline().rstrip('\n')
            while line:
                res[line] = line
                line = fp.readline().rstrip('\n')
        return res

    def invoke_extractor(self, extractor=None, doc=None, json_path=None, input_key=None, output_key=None):
        # cache parsed json_path, not a string, globally

        containers = doc.cdr_document['__content_strict']
        # containers = doc.select_containers(json_path)
        if isinstance(containers, list):
            for c in containers:
                segment = c.get(input_key)
                tokens = doc.get_tokens(segment)
        else:
            segment = containers.get(input_key)
            tokens = doc.get_tokens(segment)

        fake_extraction = [i.text for i in tokens]
        doc.store_extraction(extractor, fake_extraction, containers, output_key)
        print(json.dumps(doc.cdr_document, indent=2))
            # if extractor.requires_tokens():
            #     tokens = doc.get_tokens(segment, tokenizer=extractor.preferred_tokenizer())
            #     if tokens:
            #         extraction = extractor.extract(tokens, doc)
            #         doc.store_extraction(extractor, extraction, c, output_key)
