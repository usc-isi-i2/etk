# import unittest
# from etk.extractors.dbpedia_spotlight_extractor import DBpediaSpotlightExtractor
#
#
# class TestDBPediaSpotlightExtractor(unittest.TestCase):
#
#     def test_DBPediaSpotlight_extractor(self) -> None:
#         test_get_attr = False
#         filters = ['Person', 'Place', 'Organisation']
#         confidence = 0.5
#         extractor = DBpediaSpotlightExtractor(extractor_name='dbPedia_extractor',
#                                               search_url='http://model.dbpedia-spotlight.org/en/annotate',
#                                               get_attr=test_get_attr,
#                                               get_attr_url="http://dbpedia.org/sparql")
#         text = 'Cohen has not been charged with a crime, but the FBI raid of his home, \
#                 hotel room and office in early April revealed that prosecutors had zeroed in on \
#                 his personal financial dealings, including the payment he made to porn star \
#                 Stormy Daniels on Trump\'s behalf before the election.'
#         extracted = list()
#         results = extractor.extract(text, confidence=confidence, filter=filters)
#
#         # if not test_get_attr:
#
#         for i in results:
#             extracted_value = {
#                 'value': i.value,
#                 'confidence': i.confidence,
#                 'start_char': i.provenance['start_char'],
#                 'end_char': i.provenance['end_char'],
#             }
#             extracted.append(extracted_value)
#         expected = \
#             [
#                 {"value":
#                      {"surface_form": "FBI",
#                       "uri": "http://dbpedia.org/resource/Federal_Bureau_of_Investigation",
#                       "types":
#                           ["Wikidata:Q43229",
#                            "Wikidata:Q327333",
#                            "Wikidata:Q24229398",
#                            "DUL:SocialPerson",
#                            "DUL:Agent",
#                            "Schema:Organization",
#                            "DBpedia:Organisation",
#                            "DBpedia:GovernmentAgency",
#                            "DBpedia:Agent"],
#                       "similarity_scores": 0.9999999999998863},
#                  "confidence": 0.5,
#                  "start_char": 49,
#                  "end_char": 52},
#
#                 {"value":
#                      {"surface_form": "financial dealings",
#                       "uri": "http://dbpedia.org/resource/Yasser_Arafat",
#                       "types":
#                           ["Http://xmlns.com/foaf/0.1/Person",
#                            "Wikidata:Q82955",
#                            "Wikidata:Q5",
#                            "Wikidata:Q30461",
#                            "Wikidata:Q24229398",
#                            "Wikidata:Q215627",
#                            "DUL:NaturalPerson",
#                            "DUL:Agent",
#                            "Schema:Person",
#                            "DBpedia:President",
#                            "DBpedia:Politician",
#                            "DBpedia:Person",
#                            "DBpedia:Agent"],
#                       "similarity_scores": 0.9999999999648139},
#                  "confidence": 0.5,
#                  "start_char": 196,
#                  "end_char": 214},
#
#                 {"value":
#                      {"surface_form": "Stormy Daniels",
#                       "uri": "http://dbpedia.org/resource/Stormy_Daniels",
#                       "types":
#                           ["Http://xmlns.com/foaf/0.1/Person",
#                            "Wikidata:Q5",
#                            "Wikidata:Q488111",
#                            "Wikidata:Q483501",
#                            "Wikidata:Q33999",
#                            "Wikidata:Q24229398",
#                            "Wikidata:Q215627",
#                            "DUL:NaturalPerson",
#                            "DUL:Agent",
#                            "Schema:Person",
#                            "DBpedia:Person",
#                            "DBpedia:Artist",
#                            "DBpedia:Agent",
#                            "DBpedia:AdultActor",
#                            "DBpedia:Actor"],
#                       "similarity_scores": 1.0},
#                  "confidence": 0.5,
#                  "start_char": 275,
#                  "end_char": 289},
#
#                 {"value":
#                      {"surface_form": "Trump",
#                       "uri": "http://dbpedia.org/resource/Donald_Trump",
#                       "types":
#                           ["Http://xmlns.com/foaf/0.1/Person",
#                            "Wikidata:Q5",
#                            "Wikidata:Q24229398",
#                            "Wikidata:Q215627",
#                            "DUL:NaturalPerson",
#                            "DUL:Agent",
#                            "Schema:Person",
#                            "DBpedia:Person",
#                            "DBpedia:Agent"],
#                       "similarity_scores": 0.9888612234881796},
#                  "confidence": 0.5,
#                  "start_char": 293,
#                  "end_char": 298}]
#
#         result_count = 0
#         while result_count < len(extracted):
#             exact_form = extracted[result_count]['value']['surface_form']
#             index_form = text[extracted[result_count]['start_char']:extracted[result_count]['end_char']]
#             self.assertEqual(exact_form, index_form)
#             self.assertEqual(extracted[result_count], expected[result_count])
#             result_count += 1
#
#         # else:
#         test_get_attr = True
#         extractor = DBpediaSpotlightExtractor(extractor_name='dbPedia_extractor',
#                                               search_url='http://model.dbpedia-spotlight.org/en/annotate',
#                                               get_attr=test_get_attr,
#                                               get_attr_url="http://dbpedia.org/sparql")
#         extracted = list()
#         results = extractor.extract(text, confidence=confidence, filter=filters)
#         for i in results:
#             extracted_value = {
#                 'value': i.value,
#                 'confidence': i.confidence
#             }
#             extracted.append(extracted_value)
#         attr_len = [47, 41, 34, 55]
#         first_attr_cat = ['Federal Bureau of Investigation', 'Yasser Arafat', 'Stormy Daniels', 'Donald Trump']
#
#         result_count = 0
#         while result_count < len(extracted):
#             self.assertEqual(attr_len[result_count], len(extracted[result_count]['value']['attributes']))
#             self.assertEqual(first_attr_cat[result_count],
#                              extracted[result_count]['value']['attributes']['rdf-schema#label'][0])
#             result_count += 1
#
#
# if __name__ == '__main__':
#     unittest.main()
