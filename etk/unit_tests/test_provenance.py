import unittest
import os, sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from etk.etk import ETK
from etk.extractors.glossary_extractor import GlossaryExtractor


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

    def test_Extractable(self) -> None:

        self.etk = ETK()
        g = ['runqi', 'sylvia', 'dongyu', 'mayank', 'pedro', 'amandeep', 'yixiang']
        self.name_extractor = GlossaryExtractor(g, "name_extractor",
                                                self.etk.default_tokenizer,
                                                case_sensitive=False, ngrams=1)
        doc = self.etk.create_document(sample_input)
        descriptions = doc.select_segments("projects[*].description")
        projects = doc.select_segments("projects[*]")

        for d, p in zip(descriptions, projects):
            names = doc.invoke_extractor(self.name_extractor, d)
            p.store_extractions(names, "members")

        expected = {
            "projects": [
                {
                    "name": "etk",
                    "description": "version 2 of etk, implemented by Runqi, Dongyu, Sylvia, Amandeep and others.",
                    "members": ["Runqi", "Dongyu", "Sylvia", "Amandeep"]
                },
                {
                    "name": "rltk",
                    "description": "record linkage toolkit, implemented by Pedro, Mayank, Yixiang and several students.",
                    "members": ["Pedro", "Mayank", "Yixiang"]
                }
            ],
            "provenances": [
                {
                    "@type": "extraction_provenance_record", "@id": 0, "method": "name_extractor",
                    "confidence": 1.0, "origin_record": [
                    {"path": "projects.[0].description", "start_char": 33, "end_char": 38}
                ]
                },
                {
                    "@type": "extraction_provenance_record", "@id": 1, "method": "name_extractor",
                    "confidence": 1.0, "origin_record": [
                    {"path": "projects.[0].description", "start_char": 40, "end_char": 46
                     }
                ]
                },
                {
                    "@type": "extraction_provenance_record", "@id": 2, "method": "name_extractor",
                    "confidence": 1.0, "origin_record": [
                    {"path": "projects.[0].description", "start_char": 48, "end_char": 54
                     }
                ]
                },
                {
                    "@type": "extraction_provenance_record", "@id": 3, "method": "name_extractor",
                    "confidence": 1.0, "origin_record": [
                    {"path": "projects.[0].description", "start_char": 56, "end_char": 64
                     }
                ]
                },
                {
                    "@type": "storage_provenance_record", "doc_id": None, "field": None,
                    "destination": "projects.[0].members", "provenance_record_id": [0, 1, 2, 3]
                },
                {
                    "@type": "extraction_provenance_record", "@id": 4, "method": "name_extractor",
                    "confidence": 1.0, "origin_record": [
                    {"path": "projects.[1].description", "start_char": 39, "end_char": 44
                     }
                ]
                },
                {
                    "@type": "extraction_provenance_record", "@id": 5, "method": "name_extractor",
                    "confidence": 1.0,
                    "origin_record": [
                        {"path": "projects.[1].description", "start_char": 46, "end_char": 52
                         }
                    ]
                },
                {
                    "@type": "extraction_provenance_record", "@id": 6, "method": "name_extractor",
                    "confidence": 1.0, "origin_record": [
                    {"path": "projects.[1].description", "start_char": 54, "end_char": 61
                     }
                ]
                },
                {
                    "@type": "storage_provenance_record", "doc_id": None, "field": None,
                    "destination": "projects.[1].members", "provenance_record_id": [4, 5, 6]
                }
            ]
        }

        self.assertEqual(doc.value, expected)

if __name__ == '__main__':
    unittest.main()