from etk.extractor import Extractor
from etk.extraction import Extraction
from typing import List



class DBpediaSpotlightExtractor(Extractor):
    def __init__(self):
        # TODO initialise stuff
        # TODO if using the dbpedia webservice, a class variable should be set with the url
        # TODO if using the codebase, the library can be setup to called here
        # TODO if using the codebase, we need a local dbpedia against which the queries are to be made, set it up here as well
        pass

    def extract(self, text: str) -> List[Extraction]:
        """
            Extract with the input text
            Args:
                text: str

            Returns: List[Extraction]
        """
        # TODO add code here to call the dbpedia spotlight and to extract the entities, create a Extraction object and return a list of Extraction objects
        return

