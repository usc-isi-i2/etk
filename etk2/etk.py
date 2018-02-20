import json

class ETK(object):

    def __init__(self):
        pass

    def invoke_extractor(self, extractor=None, doc=None, json_path=None, input_key=None, output_key=None):

        containers = doc.cdr_document['__content_strict']
        # containers = doc.select_containers(json_path)
        if isinstance(containers, list):
            for c in containers:
                segment = c.get(input_key)
                tokens = doc.get_tokens(segment)
                for i in tokens:
                    print(i.orth_)
        else:
            segment = containers.get(input_key)
            tokens = doc.get_tokens(segment)
            for i in tokens:
                print(i.orth_)

        fake_extraction = doc.tokenizer.reconstruct_text(tokens)
        doc.store_extraction(extractor, fake_extraction, containers, output_key)
        print(json.dumps(doc.cdr_document, indent=2))
            # if extractor.requires_tokens():
            #     tokens = doc.get_tokens(segment, tokenizer=extractor.preferred_tokenizer())
            #     if tokens:
            #         extraction = extractor.extract(tokens, doc)
            #         doc.store_extraction(extractor, extraction, c, output_key)
