import unittest, json
from etk.etk import ETK
from etk.extractors.glossary_extractor import GlossaryExtractor
from etk.knowledge_graph_schema import KGSchema

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


class TestProvenance(unittest.TestCase):

    def test_Provenance(self) -> None:
        kg_schema = KGSchema(json.load(open('etk/unit_tests/ground_truth/test_config.json')))

        self.etk = ETK(kg_schema=kg_schema)
        g = ['runqi', 'sylvia', 'dongyu', 'mayank', 'pedro', 'amandeep', 'yixiang']
        self.name_extractor = GlossaryExtractor(g, "name_extractor",
                                                self.etk.default_tokenizer,
                                                case_sensitive=False, ngrams=1)
        doc = self.etk.create_document(sample_input)
        descriptions = doc.select_segments("projects[*].description")
        projects = doc.select_segments("projects[*]")

        for d, p in zip(descriptions, projects):
            names = doc.extract(self.name_extractor, d)
            p.store(names, "members")

        expected_provenances = [
            {
              "@id": 0,
              "@type": "extraction_provenance_record",
              "method": "name_extractor",
              "confidence": 1.0,
              "origin_record": {
                "path": "projects.[0].description",
                "start_char": 33,
                "end_char": 38
              }
            },
            {
              "@id": 1,
              "@type": "extraction_provenance_record",
              "method": "name_extractor",
              "confidence": 1.0,
              "origin_record": {
                "path": "projects.[0].description",
                "start_char": 40,
                "end_char": 46
              }
            },
            {
              "@id": 2,
              "@type": "extraction_provenance_record",
              "method": "name_extractor",
              "confidence": 1.0,
              "origin_record": {
                "path": "projects.[0].description",
                "start_char": 48,
                "end_char": 54
              }
            },
            {
              "@id": 3,
              "@type": "extraction_provenance_record",
              "method": "name_extractor",
              "confidence": 1.0,
              "origin_record": {
                "path": "projects.[0].description",
                "start_char": 56,
                "end_char": 64
              }
            },
            {
              "@id": 4,
              "@type": "storage_provenance_record",
              "doc_id": None,
              "field": None,
              "destination": "projects.[0].members",
              "parent_provenances": {
                "Runqi": 0,
                "Dongyu": 1,
                "Sylvia": 2,
                "Amandeep": 3
              }
            },
            {
              "@id": 5,
              "@type": "extraction_provenance_record",
              "method": "name_extractor",
              "confidence": 1.0,
              "origin_record": {
                "path": "projects.[1].description",
                "start_char": 39,
                "end_char": 44
              }
            },
            {
              "@id": 6,
              "@type": "extraction_provenance_record",
              "method": "name_extractor",
              "confidence": 1.0,
              "origin_record": {
                "path": "projects.[1].description",
                "start_char": 46,
                "end_char": 52
              }
            },
            {
              "@id": 7,
              "@type": "extraction_provenance_record",
              "method": "name_extractor",
              "confidence": 1.0,
              "origin_record": {
                "path": "projects.[1].description",
                "start_char": 54,
                "end_char": 61
              }
            },
            {
              "@id": 8,
              "@type": "storage_provenance_record",
              "doc_id": None,
              "field": None,
              "destination": "projects.[1].members",
              "parent_provenances": {
                "Pedro": 5,
                "Mayank": 6,
                "Yixiang": 7
              }
            }
            ]
        expected_projects = [
                {
                  "name": "etk",
                  "description": "version 2 of etk, implemented by Runqi, Dongyu, Sylvia, Amandeep and others.",
                  "members": [
                    "Runqi",
                    "Dongyu",
                    "Sylvia",
                    "Amandeep"
                  ]
                },
                {
                  "name": "rltk",
                  "description": "record linkage toolkit, implemented by Pedro, Mayank, Yixiang and several students.",
                  "members": [
                    "Pedro",
                    "Mayank",
                    "Yixiang"
                  ]
                }
              ]
        #print ("hiiiiiiiiiiiiiiiii")
        #print ("projects: " + str(doc.value["projects"]))
        #print ("provenances: " + str(doc.value["provenances"]))
        self.assertEqual(expected_projects, doc.value["projects"])
        self.assertEqual(expected_provenances, doc.value["provenances"])

if __name__ == '__main__':
    unittest.main()
