import json, os, sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from etk.etk import ETK
from etk.extractors.glossary_extractor import GlossaryExtractor
from etk.etk_module import ETKModule

from pyrallel import ParallelProcessor # pip install pyrallel.lib


class ExampleETKModule(ETKModule):
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
        return list()


if __name__ == "__main__":
    sample_input = [
        {"projects": [
            {
                "name": "etk",
                "description": "version 2 of etk, implemented by Runqi, Dongyu, Sylvia, Amandeep, Anika and others."
        }]},
        {"projects": [
            {
                "name": "rltk",
                "description": "record linkage toolkit, implemented by Pedro, Mayank, Yixiang and several students."
        }]}
    ]

    etk = ETK(modules=ExampleETKModule)
    extractions = []

    def mapper(sample, _idx):
        doc = etk.create_document(sample)
        docs = etk.process_ems(doc)
        sys.stdout.flush()
        re = docs[0].value
        # print(re)
        return re

    def collect(extracted):
        extractions.append(extracted)

    pp = ParallelProcessor(2, mapper=mapper, collector=collect, enable_process_id=True)
    pp.start()
    for sample in sample_input:
        pp.add_task(sample)
    pp.task_done()
    pp.join()
    print(extractions)
