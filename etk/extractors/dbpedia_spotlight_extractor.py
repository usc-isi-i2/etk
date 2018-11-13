from SPARQLWrapper import SPARQLWrapper, JSON
from collections import OrderedDict
from etk.extractor import Extractor, InputType
from etk.extraction import Extraction
from typing import List
import requests


class DBpediaSpotlightExtractor(Extractor):
    """
    **Description**
        This extractor takes a string of text as input, uses DBPedia API to annotate words and phrases in the text input.

    Examples:
        ::

            dbpedia_spotlight_extractor = DBpediaSpotlightExtractor(search_url='http://model.dbpedia-spotlight.org/en/annotate',
                                                                    get_attr=False,
                                                                    get_attr_url="http://dbpedia.org/sparql")
            dbpedia_spotlight_extractor.extract(text=input_doc,
                                                filter=['Person', 'Place', 'Organisation'])

    """

    def __init__(self, extractor_name: str, search_url: str, get_attr=False,
                 get_attr_url="http://dbpedia.org/sparql"):
        Extractor.__init__(self, input_type=InputType.TEXT,
                           category="built_in_extractor",
                           name=extractor_name)
        self._search_url = search_url
        self._get_attr = get_attr
        self._get_attr_url = get_attr_url
        self.seen_uris = dict()

    def extract(self, text: str, confidence=0.5, filter=['Person', 'Place', 'Organisation']) -> List[Extraction]:
        """
            Extract with the input text, confidence and fields filter to be used.
            Args:
                text (str): text input to be annotated
                confidence (float): the confidence of the annotation
                filter (List[str]): the fields that to be extracted

            Returns:
                List[Extraction]
        """

        filter = ','.join(filter)
        search_data = [('confidence', confidence),
                       ('text', text),
                       ('types', filter)]
        search_headers = {'Accept': 'application/json'}
        r = requests.post(self._search_url,
                          data=search_data,
                          headers=search_headers)
        results = r.json()

        last_results = self._combiner(results)
        return last_results

    @staticmethod
    def deduplicate_by_uri(resources):
        dedup = dict()
        for resource in resources:
            if resource['@URI'] not in dedup:
                dedup[resource['@URI']] = resource['@surfaceForm']

            if len(resource['@surfaceForm']) >= len(dedup[resource['@URI']]):
                dedup[resource['@URI']] = resource['@surfaceForm']
            else:
                del resource
        return resources

    def _combiner(self, results: dict) -> List[Extraction]:
        return_result = list()
        if "Resources" in results:
            resources_results = self.deduplicate_by_uri(results["Resources"])

            for one_result in resources_results:
                types = one_result['@types'].split(',')
                values = {'surface_form': one_result['@surfaceForm'],
                          'uri': one_result['@URI'],
                          'types': types,
                          'similarity_scores': float(one_result['@similarityScore'])}
                if self._get_attr:
                    if one_result['@URI'] not in self.seen_uris:
                        # attr = self._attr_finder(one_result['@URI'])
                        attr = self._en_label_finder(one_result['@URI'])

                        self.seen_uris[one_result['@URI']] = attr
                    values['attributes'] = self.seen_uris[one_result['@URI']]
                    values['surface_form'] = self.seen_uris[one_result['@URI']]['rdf-schema#label']

                return_result.append(Extraction(confidence=float(results['@confidence']),
                                                extractor_name=self.name,
                                                start_char=int(one_result['@offset']),
                                                end_char=int(one_result['@offset']) + len(
                                                    one_result['@surfaceForm']),
                                                value=values))

            return return_result
        return list()

    def _en_label_finder(self, uri):
        sparql = SPARQLWrapper(self._get_attr_url)
        sparql.setQuery("SELECT distinct * WHERE {<" + uri + "> "
                                                             "<http://www.w3.org/2000/01/rdf-schema#label> "
                                                             "?resource  FILTER(langMatches(lang(?resource),'en'))}")
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        attr = OrderedDict()
        attr_key = 'rdf-schema#label'
        for one_item in results['results']['bindings']:
            attr_val = one_item['resource']['value']
            if attr_key not in attr:
                attr[attr_key] = [attr_val]
            else:
                attr[attr_key].append(attr_val)
        return attr

    def _attr_finder(self, uri) -> dict:
        sparql = SPARQLWrapper(self._get_attr_url)
        sparql.setQuery("SELECT distinct * WHERE {<" + uri + "> ?link ?resource}")
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()

        attr = OrderedDict()
        cnt_attr = 0
        for one_item in results['results']['bindings']:
            if ('xml:lang' in one_item['resource']) and (one_item['resource']['xml:lang'] != 'en'):
                pass
            else:
                attr_key = one_item['link']['value'].split('/')[-1]
                attr_val = one_item['resource']['value']
                if attr_key not in attr:
                    if cnt_attr < 100:
                        attr[attr_key] = [attr_val]
                        cnt_attr += 1
                else:
                    attr[attr_key].append(attr_val)
        return attr
