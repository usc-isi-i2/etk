import unittest
import json
from etk.extractors.dbpedia_spotlight_extractor import DBpediaSpotlightExtractor


class TestDBPediaSpotlightExtractor(unittest.TestCase):

    def test_DBPediaSpotlight_extractor(self) -> None:
        text = 'Cohen has not been charged with a crime, but the FBI raid of his home, \
        hotel room and office in early April revealed that prosecutors had zeroed in on \
        his personal financial dealings, including the payment he made to porn star \
        Stormy Daniels on Trump\'s behalf before the election.'
        filters = ['Person', 'Place', 'Organisation']
        confidence = 0.5

        extractor = DBpediaSpotlightExtractor(extractor_name='dbPedia_extractor',
                                              search_url='http://model.dbpedia-spotlight.org/en/annotate')
        results = extractor.extract(text, confidence=confidence, filter=filters)

        extracted = list()
        for i in results:
            extracted_value = {
                'value': i.value,
                'confidence': i.confidence,
                'start_char': i.provenance['start_char'],
                'end_char': i.provenance['end_char'],
            }
            extracted.append(extracted_value)
        expected = \
            [
                {"value":
                    {"surface_form": "FBI",
                     "uri": "http://dbpedia.org/resource/Federal_Bureau_of_Investigation",
                     "types":
                        ["Wikidata:Q43229",
                         "Wikidata:Q327333",
                         "Wikidata:Q24229398",
                         "DUL:SocialPerson",
                         "DUL:Agent",
                         "Schema:Organization",
                         "DBpedia:Organisation",
                         "DBpedia:GovernmentAgency",
                         "DBpedia:Agent"],
                     "similarity_scores": 0.9999999999998863},
                 "confidence": 0.5,
                 "start_char": 49,
                 "end_char": 52},

                {"value":
                    {"surface_form": "financial dealings",
                     "uri": "http://dbpedia.org/resource/Yasser_Arafat",
                     "types":
                        ["Http://xmlns.com/foaf/0.1/Person",
                         "Wikidata:Q82955",
                         "Wikidata:Q5",
                         "Wikidata:Q30461",
                         "Wikidata:Q24229398",
                         "Wikidata:Q215627",
                         "DUL:NaturalPerson",
                         "DUL:Agent",
                         "Schema:Person",
                         "DBpedia:President",
                         "DBpedia:Politician",
                         "DBpedia:Person",
                         "DBpedia:Agent"],
                     "similarity_scores": 0.9999999999648139},
                 "confidence": 0.5,
                 "start_char": 180,
                 "end_char": 198},

                {"value":
                    {"surface_form": "Stormy Daniels",
                     "uri": "http://dbpedia.org/resource/Stormy_Daniels",
                     "types":
                        ["Http://xmlns.com/foaf/0.1/Person",
                         "Wikidata:Q5",
                         "Wikidata:Q488111",
                         "Wikidata:Q483501",
                         "Wikidata:Q33999",
                         "Wikidata:Q24229398",
                         "Wikidata:Q215627",
                         "DUL:NaturalPerson",
                         "DUL:Agent",
                         "Schema:Person",
                         "DBpedia:Person",
                         "DBpedia:Artist",
                         "DBpedia:Agent",
                         "DBpedia:AdultActor",
                         "DBpedia:Actor"],
                     "similarity_scores": 1.0},
                 "confidence": 0.5,
                 "start_char": 251,
                 "end_char": 265},

                {"value":
                    {"surface_form": "Trump",
                     "uri": "http://dbpedia.org/resource/Donald_Trump",
                     "types":
                        ["Http://xmlns.com/foaf/0.1/Person",
                         "Wikidata:Q5",
                         "Wikidata:Q24229398",
                         "Wikidata:Q215627",
                         "DUL:NaturalPerson",
                         "DUL:Agent",
                         "Schema:Person",
                         "DBpedia:Person",
                         "DBpedia:Agent"],
                     "similarity_scores": 0.9888612234881796},
                 "confidence": 0.5,
                 "start_char": 269,
                 "end_char": 274}]

        result_count = 0
        while result_count < len(extracted):
            exact_form = extracted[result_count]['value']['surface_form']
            index_form = text[extracted[result_count]['start_char']:extracted[result_count]['end_char']]
            self.assertEqual(exact_form, index_form)
            self.assertEqual(extracted[result_count], expected[result_count])
            result_count += 1


if __name__ == '__main__':
    unittest.main()
