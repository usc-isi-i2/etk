from etk.etk import ETK
from etk.knowledge_graph import KGSchema
from etk.extractors.glossary_extractor import GlossaryExtractor
from etk.etk_module import ETKModule
from etk.wikidata import *


class ExampleETKModule(ETKModule):
    """
    Abstract class for extraction module
    """
    def __init__(self, etk):
        ETKModule.__init__(self, etk)
        self.name_extractor = GlossaryExtractor(self.etk.load_glossary("./names.txt"), "name_extractor",
                                                self.etk.default_tokenizer, case_sensitive=False, ngrams=1)

    def process_document(self, doc):
        """
        Douglas_Adams educated_at
            value: St_John's_College
            qualifier: start_time 1971
            qualifier: end_time 1974
            reference: stated_in Encyclopædia_Britannica_Online
            rank: normal
        """
        for k, v in wiki_namespaces.items():
            doc.kg.bind(k, v)

        p = WDProperty('C3001', Datatype.QuantityValue)
        p.add_label('violent crime offenses', lang='en')
        p.add_description("number of violent crime offenses reported by the sheriff's office or county police department",
                          lang='en')
        p.add_statement('P31', Item('D1001'))
        p.add_statement('P1629', Item('Q1520311'))

        doc.kg.add_subject(p)
        return list()


if __name__ == "__main__":
    kg_schema = KGSchema()
    kg_schema.add_schema('@prefix : <http://isi.edu/> .', 'ttl')
    etk = ETK(kg_schema=kg_schema, modules=ExampleETKModule)
    doc = etk.create_document({}, doc_id="http://isi.edu/default-ns/projects")

    docs = etk.process_ems(doc)

    print(docs[0].kg.serialize('ttl'))
    with open('p.tsv', 'w') as fp:
        serialize_change_record(fp)

