import unittest, json
from etk.extractors.email_extractor import EmailExtractor
from etk.etk import ETK
from etk.knowledge_graph_schema import KGSchema


class TestEmailExtractor(unittest.TestCase):

    def test_EmailExtractor(self) -> None:
        kg_schema = KGSchema(json.load(open('etk/unit_tests/ground_truth/test_config.json')))

        etk = ETK(kg_schema=kg_schema)

        text = "runqisha@usc.edu 231.45.se.ere@gmail.com " \
               "3233@no.s nefwe.de-ef@l.net  E-mail:anna@wanadoo.fr.com office@fantasie-escort.at"

        email_extractor = EmailExtractor(
            nlp=etk.default_nlp,
            tokenizer=etk.default_tokenizer,
            extractor_name="email_extractor"
        )

        extractions = email_extractor.extract(text)

        extracted = []
        for i in extractions:
            extracted_value = {
                "value": i.value,
                "start_char": i.provenance["start_char"],
                "end_char": i.provenance["end_char"],
                "value_from_text": text[i.provenance["start_char"]: i.provenance["end_char"]]
            }
            extracted.append(extracted_value)
            self.assertEqual(extracted_value["value"], extracted_value["value_from_text"])

        expected = [{'value': 'office@fantasie-escort.at', 'start_char': 97, 'end_char': 122,
                     'value_from_text': 'office@fantasie-escort.at'},
                    {'value': 'runqisha@usc.edu', 'start_char': 0, 'end_char': 16,
                     'value_from_text': 'runqisha@usc.edu'},
                    {'value': 'anna@wanadoo.fr.com', 'start_char': 77, 'end_char': 96,
                     'value_from_text': 'anna@wanadoo.fr.com'},
                    {'value': '231.45.se.ere@gmail.com', 'start_char': 17, 'end_char': 40,
                     'value_from_text': '231.45.se.ere@gmail.com'},
                    {'value': 'nefwe.de-ef@l.net', 'start_char': 51, 'end_char': 68,
                     'value_from_text': 'nefwe.de-ef@l.net'}]

        self.assertEqual(sorted(expected, key=lambda x: x["start_char"]),
                         sorted(extracted, key=lambda x: x["start_char"]))