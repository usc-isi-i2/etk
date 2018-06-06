import json, os, sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from etk.etk import ETK
from etk.extractors.glossary_extractor import GlossaryExtractor
from etk.etk_module import ETKModule
from etk.provenance_api import ProvenanceAPI
from etk.knowledge_graph import KGSchema

class ProvenanceOriginExtractionETKModule(ETKModule):
    """
    Abstract class for extraction module
    """
    def __init__(self, etk):
        ETKModule.__init__(self, etk)
        self.name_extractor = GlossaryExtractor(self.etk.load_glossary("./names.txt"), "name_extractor",
                                                self.etk.default_tokenizer,
                                                case_sensitive=False, ngrams=1)
        self.student_extractor = GlossaryExtractor(self.etk.load_glossary("./student.txt"), "student_extractor",
                                                   self.etk.default_tokenizer, case_sensitive=False, ngrams=1)

    def process_document(self, doc):
        """
        Add your code for processing the document
        """

        descriptions = doc.select_segments("projects[*].description")
        projects = doc.select_segments("projects[*]")

        for d, p in zip(descriptions, projects):

            # First phase of extraction
            names = doc.extract(self.name_extractor, d)
            p.store(names, "members")

            # Second phase of extraction
            students = []
            for name_extraction in names:
                students += doc.extract(self.student_extractor, name_extraction)
            p.store(students, "students")
        doc.kg.add_value("developer", json_path="projects[*].members[*]")
        doc.kg.add_value("owner", json_path="projects[*].students[*]")
        return list()


if __name__ == "__main__":

    sample_input = {
        "projects": [
            {
                "name": "etk",
                "description": "version 2 of etk, implemented by Runqi, Dongyu, Sylvia, Amandeep, Anika and others."
            },
            {
                "name": "rltk",
                "description": "record linkage toolkit, implemented by Pedro, Mayank, Yixiang and several students."
            }
        ]
    }

    #provenance use example case for fetching origins

    kg_schema = KGSchema(json.load(open("master_config.json", "r")))
    etk = ETK(kg_schema=kg_schema, modules=ProvenanceOriginExtractionETKModule)
    doc = etk.create_document(sample_input)

    doc_ = etk.process_ems(doc)
    print(json.dumps(doc.kg.value, indent=2))
    provenanceAPI = ProvenanceAPI(doc)
    #print(json.dumps(doc.value, indent=2))

    #Use case/Example 1
    origins = provenanceAPI.get_origins("developer", 1)

    print ("Use case/Example 1: ")
    for origin in origins:
        print ("start char: " + str(origin.start_char))
        print ("end char: " + str(origin.end_char))
        print ("jsonPath char: " + str(origin.json_path))

    #Use case/Example 2
    origins = provenanceAPI.get_origins("developer")

    print ("Use case/Example 2: ")
    for origin in origins:
        print ("start char: " + str(origin.start_char))
        print ("end char: " + str(origin.end_char))
        print ("jsonPath char: " + str(origin.json_path))

    #Use case/Example 3
    origins = provenanceAPI.get_origins(None, None, "Mayank")

    print ("Use case/Example 3: ")
    for origin in origins:
        print ("start char: " + str(origin.start_char))
        print ("end char: " + str(origin.end_char))
        print ("jsonPath char: " + str(origin.json_path))
