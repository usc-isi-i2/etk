from etk.etk_module import ETKModule
from etk.document import Document
from etk.extractors.glossary_extractor import GlossaryExtractor


class ETKModuleGlossary(ETKModule):
    def __init__(self, etk):
        ETKModule.__init__(self, etk)
        self.name_extractor = GlossaryExtractor(self.etk.load_glossary("./extraction_modules/resources/names.txt"),
                                           "name_extractor",
                                           self.etk.default_tokenizer,
                                           case_sensitive=False,
                                           ngrams=1)

        self.student_extractor = GlossaryExtractor(self.etk.load_glossary("./extraction_modules/resources/student.txt"),
                                              "student_extractor",
                                              self.etk.default_tokenizer,
                                              case_sensitive=False,
                                              ngrams=1)

    def process_document(self, doc: Document):

        descriptions = doc.select_segments("projects[*].description")
        projects = doc.select_segments("projects[*]")

        for d, p in zip(descriptions, projects):
            names = doc.extract(self.name_extractor, d)
            p.store(names, "members")

            students = []
            for name_extraction in names:
                students += doc.extract(self.student_extractor, name_extraction)
            p.store(students, "students")

        doc.kg.add_value("developer", json_path="projects[*].members[*]")
        doc.kg.add_value("student_developer", json_path="projects[*].students[*]")

        doc.kg.add_value("id", json_path='$.doc_id')
