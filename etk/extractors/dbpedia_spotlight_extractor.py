from etk.extractor import Extractor, InputType
from etk.extraction import Extraction
from typing import List
import requests


class DBpediaSpotlightExtractor(Extractor):
    search_url = ''

    def __init__(self, extractor_name: str, url: str):
        Extractor.__init__(self, input_type=InputType.TEXT,
                           category="build_in_extractor",
                           name=extractor_name)
        self.search_url = url
        # TODO initialise stuff
        # TODO if using the dbpedia webservice, a class variable should be set with the url
        # TODO if using the codebase, the library can be setup to called here
        # TODO if using the codebase, we need a local dbpedia against which the queries are to be made, set it up here as well

    def extract(self, text: str, confidence: float, filter: list) -> List[Extraction]:
        """
            Extract with the input text
            Args:
                text: str

            Returns: List[Extraction]
        """

        self.filter = ','.join(filter)

        # url = 'http://api.dbpedia-spotlight.org/en/annotate'

        # url = 'http://model.dbpedia-spotlight.org/en/annotate'
        # filters='PREFIX dbo: <http://dbpedia.org/ontology/> '+'SELECT DISTINCT ?'+filter+' WHERE  { ?'+filter+' rdf:type dbo:'+filter +'}'
        # search_data = [('confidence', self.search_confidence), ('text', text),('sparql',filters)]

        search_data = [('confidence', confidence),
                       ('text', text),
                       ('types', self.filter)]
        search_headers = {'Accept': 'application/json'}

        r = requests.post(self.search_url,
                          data=search_data,
                          headers=search_headers)
        results = r.json()
        last_results = self.combiner(results)
        # TODO add code here to call the dbpedia spotlight and to extract the entities, create a Extraction object and return a list of Extraction objects
        return last_results

    # combiner function is used to form results in to Extraction object
    def combiner(self, results: dict) -> List[Extraction]:
        return_result = []
        if "Resources" in results:
            resources_results = results["Resources"]
            for one_result in resources_results:
                types = one_result['@types'].split(',')
                return_result.append(Extraction(confidence=float(results['@confidence']),
                                                extractor_name=self.name,
                                                start_char=int(one_result['@offset']),
                                                end_char=int(one_result['@offset']) + len(one_result['@surfaceForm']),
                                                value={'surfaceForm': one_result['@surfaceForm'],
                                                       'URI': one_result['@URI'],
                                                       'types': types,
                                                       'similarityScore': float(one_result['@similarityScore'])}))
            return return_result
        return []
