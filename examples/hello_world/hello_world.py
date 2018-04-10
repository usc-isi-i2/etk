import json, os, sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from etk.etk import ETK
from etk.extractors.glossary_extractor import GlossaryExtractor
from etk.extraction_module import ExtractionModule


class HelloWorldExtractionModule(ExtractionModule):
    """
    Abstract class for extraction module
    """
    def __init__(self, etk):
        ExtractionModule.__init__(self, etk)
        self.name_extractor = GlossaryExtractor(self.etk.load_glossary("./names.txt"), "name_extractor",
                                                self.etk.default_tokenizer,
                                                case_sensitive=False, ngrams=1)

    def process_document(self, doc):
        """
        Add your code for processing the document
        """

        descriptions = doc.select_segments("projects[*].description")
        projects = doc.select_segments("projects[*]")

        for d, p in zip(descriptions, projects):
            names = doc.invoke_extractor(self.name_extractor, d)
            p.store_extractions(names, "members")


if __name__ == "__main__":

    # example for glossary extractor:

    sample_input = {
        "projects": [
            {
                "name": "etk",
                "description": "version 2 of etk, implemented by Runqi, Dongyu, Sylvia, Amandeep and others."
            },
            {
                "name": "rltk",
                "description": "record linkage toolkit, implemented by Pedro, Mayank, Yixiang and several students."
            }
        ]
    }

    etk = ETK(modules=HelloWorldExtractionModule)
    doc = etk.create_document(sample_input)

    doc, _ = etk.process_ems(doc)

    print(json.dumps(doc.value, indent=2))
