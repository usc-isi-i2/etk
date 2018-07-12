from etk.extractor import Extractor, InputType
from etk.extraction import Extraction
from typing import List
import requests


class DBpediaSpotlightExtractor(Extractor):

    def __init__(self, extractor_name: str, search_url: str):
        Extractor.__init__(self, input_type=InputType.TEXT,
                           category="built_in_extractor",
                           name=extractor_name)
        self.search_url = search_url

    def extract(self, text: str, confidence=0.5, filter=['Person', 'Place', 'Organisation']) -> List[Extraction]:
        """
            Extract with the input text, confidence and fields filter to be used.
            Args:
                text: str, text input to be annotated
                confidence: float, the confidence of the annotation
                filter: list, the fields that to be extracted
            Returns: List[Extraction]
        """

        filter = ','.join(filter)
        search_data = [('confidence', confidence),
                       ('text', text),
                       ('types', filter)]
        search_headers = {'Accept': 'application/json'}
        r = requests.post(self.search_url,
                          data=search_data,
                          headers=search_headers)
        results = r.json()
        last_results = self.combiner(results)
        return last_results

    def combiner(self, results: dict) -> List[Extraction]:
        return_result = list()
        if "Resources" in results:
            resources_results = results["Resources"]
            for one_result in resources_results:
                types = one_result['@types'].split(',')
                return_result.append(Extraction(confidence=float(results['@confidence']),
                                                extractor_name=self.name,
                                                start_char=int(one_result['@offset']),
                                                end_char=int(one_result['@offset']) + len(
                                                    one_result['@surfaceForm']),
                                                value={'surface_form': one_result['@surfaceForm'],
                                                       'uri': one_result['@URI'],
                                                       'types': types,
                                                       'similarity_scores': float(one_result['@similarityScore'])}))
            return return_result
        return list()
