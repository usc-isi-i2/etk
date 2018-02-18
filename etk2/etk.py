from tokenizer import Tokenizer

class ETK(object):

    def invoke_extractor(self, extractor, doc, json_path, input_key, output_key):

        # containers = doc.select_containers(json_path)
        # for c in containers:
        #     segment = c.get(input_key)
        #     if extractor.requires_tokens():
        #         tokens = doc.get_tokens(segment, tokenizer=extractor.preferred_tokenizer())
        #         if tokens:
        #             extraction = extractor.extract(tokens, doc)
        #             doc.store_extraction(extractor, extraction, c, output_key)
        pass

    def get_tokens(self, text):
        t = Tokenizer()
        tokens = t.tokenize(text)
        return t.tokenize(text)