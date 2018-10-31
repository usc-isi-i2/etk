from typing import List
from etk.extraction import Extraction
from etk.extractor import Extractor, InputType
import urllib,json

class WikifierExtractor(Extractor):
    """
    Extract individual sentences using lightweight spaCy module.
    """

    def __init__(self, extractor_name: str = None, url: str = "http://www.wikifier.org/annotate-article", token: str = "navzolcwvfapmawbbtucfmtvvyrqgc", threshold: float = 0.8) -> None:
        Extractor.__init__(self,
                           input_type=InputType.TEXT,
                           category="Entity Extractions",
                           name=extractor_name if extractor_name else "Wikifer extractor")
        self.url=url
        self.token = token
        self.threshold = threshold



    def extract(self, text: str) -> List[Extraction]:
        """
        Splits text by sentences.

        Args:
            text (str): Input text to be extracted.

        Returns:
            List[Extraction]: the list of extraction or the empty list if there are no matches.
        """

        results = self.callWikifier(text=text, url=self.url, token=self.token, lang='en',threshold=self.threshold)

        extractions = list()
        if 'annotations' in results:
            for entry in results['annotations']:
                val = entry
                this_extraction = Extraction(value=val,
                                             confidence=val['support'][0]['prbConfidence'],
                                             extractor_name=self.name,
                                             start_token=val['support'][0]['chFrom'],
                                             end_token=val['support'][0]['chTo'],
                                             start_char=text[val['support'][0]['chFrom']],
                                             end_char=text[val['support'][0]['chTo']])
                extractions.append(this_extraction)
        return extractions

    def callWikifier(self,text, url,token,lang="en", threshold=0.8):
        # Prepare the URL.
        data = urllib.parse.urlencode([
            ("text", text), ("lang", lang),
            ("userKey", token),
            ("pageRankSqThreshold", "%g" % threshold), ("applyPageRankSqThreshold", "true"),
            ("nTopDfValuesToIgnore", "200"), ("nWordsToIgnoreFromList", "200"),
            ("wikiDataClasses", "true"), ("wikiDataClassIds", "false"),
            ("support", "true"), ("ranges", "false"),
            ("includeCosines", "false"), ("maxMentionEntropy", "3")
        ])
        if not url:
            url = "http://www.wikifier.org/annotate-article"
        # Call the Wikifier and read the response.
        req = urllib.request.Request(url, data=data.encode("utf8"), method="POST")
        with urllib.request.urlopen(req, timeout=60) as f:
            response = f.read()
            response = json.loads(response.decode("utf8"))
        # Output the annotations.
        return response
