from etk.extraction_module import ExtractionModule
from etk.document import Document
from etk.extractors.glossary_extractor import GlossaryExtractor


class ExtractionModuleGlossary(ExtractionModule):
    def __init__(self, etk):
        ExtractionModule.__init__(self, etk)

    def process_document(self, doc: Document):
        name_extractor = GlossaryExtractor(self.etk.load_glossary("./extraction_modules/resources/names.txt"),
                                           "name_extractor",
                                           self.etk.default_tokenizer,
                                           case_sensitive=False,
                                           ngrams=1)

        student_extractor = GlossaryExtractor(self.etk.load_glossary("./extraction_modules/resources/student.txt"),
                                              "student_extractor",
                                              self.etk.default_tokenizer,
                                              case_sensitive=False,
                                              ngrams=1)

        descriptions = doc.select_segments("projects[*].description")
        projects = doc.select_segments("projects[*]")

        for d, p in zip(descriptions, projects):
            names = doc.invoke_extractor(name_extractor, d)
            p.store_extractions(names, "members")

            students = []
            for name_extraction in names:
                students += doc.invoke_extractor(student_extractor, name_extraction)
            p.store_extractions(students, "students")

        doc.kg.add_doc_value("developer", "projects[*].members[*]")
        doc.kg.add_doc_value("student_developer", "projects[*].students[*]")
