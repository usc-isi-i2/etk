import os, sys, json
import spacy
import pandas as pd
import re

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from etk.etk_module import ETKModule
from etk.extractors.date_extractor import DateExtractor
from etk.document_selector import DefaultDocumentSelector
from etk.document import Document
from etk.knowledge_graph import KGSchema
from etk.etk import ETK


class DocRetrieveProcessor(object):

    def __init__(self, etk, nlp: None, ifp_id: str, ifp_title: str, orig_ifp_title: str) -> None:
        self.etk = etk
        self.ifp_id = ifp_id
        self.ifp_title = ifp_title
        self.nlp = spacy.load('en_core_web_lg') if not nlp else nlp
        self.query_tokens = self.nlp(self.ifp_title)
        self.orig_ifp_title = orig_ifp_title

        sentence_tokenizer = '(?<!\w\.\w)(?<![A-Z][a-z]\.)(?<=\.|\?)\s+(?=[A-Z])'
        self.sentence_tokenizer_pattern = re.compile(sentence_tokenizer)

    # process document sentence by sentence
    def process_by_sentence(self, doc: Document, threshold: float) -> Document:
        json_obj = doc.cdr_document

        # secondary key
        timestamp = json_obj['@timestamp']
        date = pd.to_datetime(timestamp, infer_datetime_format=True)
        doc_id = json_obj['doc_id']
        content = json_obj['lexisnexis']['doc_description'].replace('\n', " ").replace("[\\t\\n\\r]+", " ")
        sentences = self.sentence_tokenizer_pattern.split(content)

        scores = list(map(self.query_tokens.similarity, list(map(self.nlp, sentences))))
        max_score = max(scores)
        max_score_idx = scores.index(max_score)
        max_score_sentence = sentences[max_score_idx]
        if max_score > threshold:
            output_cdr_doc = {
                'type': 'News/IFP Relevance',
                'date': timestamp,
                'ifp': self.orig_ifp_title,
                'news_story': doc_id,
                'similarity': max_score,
                'matched_sentence': max_score_sentence
            }
            return self.etk.create_document(output_cdr_doc)
        return None

    def process_by_title(self, doc: Document, threshold: float) -> Document:
        json_obj = doc.cdr_document

        # secondary key
        timestamp = json_obj['@timestamp']
        date = pd.to_datetime(timestamp, infer_datetime_format=True)
        doc_id = json_obj['doc_id']

        title = json_obj['lexisnexis']['doc_title']
        title_token = self.nlp(title)
        similarity = self.query_tokens.similarity(title_token)

        if similarity > threshold:
            output_obj = {
                'type': 'News/IFP Relevance',
                'date': timestamp,
                'ifp': self.orig_ifp_title,
                'news_story': doc_id,
                'title': title,
                'similarity': similarity
            }
            return self.etk.create_document(output_obj)
        return None


class IFPRankingModule(ETKModule):
    def __init__(self, etk):
        ETKModule.__init__(self, etk)
        self.date_extractor = DateExtractor(self.etk, 'ifp_date_parser')
        ifp_list = open('new_ifps.jl').readlines()
        self.new_ifps = dict()
        for ifp in ifp_list:
            j = json.loads(ifp)
            self.new_ifps[j['ifp']['id']] = j['ifp']['name']
        self.parsed_ifps = dict()
        # self.ifps_entity_map = dict()
        self.threshold = 0.86
        self.nlp = spacy.load('en_core_web_lg')
        self.preprocess_ifps()
        self.ranking_criteria = 'SENTENCE'

    def preprocess_ifps(self):
        for id, ifp_name in self.new_ifps.items():

            # remove date information from query term
            extracted_date = self.date_extractor.extract(text=ifp_name)
            start, end = float('inf'), -1
            for i in extracted_date:
                start = min(start, i.provenance['start_char'])
                end = max(end, i.provenance['end_char'])
            # delete date from query term
            if len(extracted_date) != 0:
                parsed_ifp_name = ifp_name[:start] + ifp_name[end + 1:]
                self.parsed_ifps[id] = parsed_ifp_name
            else:
                self.parsed_ifps[id] = ifp_name

            # TODO use this code in future if news articles have to be matched after filtering out using entities in the IFP
            # self.ifps_entity_map[ifp] = list()
            # extract entities from query term
            # doc = self.nlp(ifp)
            # for ent in doc.ents:
            #     self.ifps_entity_map[ifp].append(re.escape(ent.text.strip()))
            # # remove empty entities
            # self.ifps_entity_map[ifp] = list(filter(bool, self.ifps_entity_map[ifp]))

    def process_document(self, doc: Document):
        result_docs = list()
        for id, parsed_ifp_name in self.parsed_ifps.items():
            dr_processor = DocRetrieveProcessor(etk=self.etk, ifp_id=id, ifp_title=parsed_ifp_name,
                                                orig_ifp_title=self.new_ifps[id], nlp=self.nlp)

            processed_doc = None
            if self.ranking_criteria == 'SENTENCE':
                processed_doc = dr_processor.process_by_sentence(doc=doc, threshold=self.threshold)
            elif self.ranking_criteria == 'TITLE':
                processed_doc = dr_processor.process_by_title(doc=doc, threshold=self.threshold)

            if processed_doc:
                for key in processed_doc.cdr_document.keys():
                    processed_doc.kg.add_value(key, processed_doc.cdr_document[key])
                result_docs.append(processed_doc)
        return result_docs

    def document_selector(self, doc) -> bool:
        """
        Boolean function for selecting document
        Args:
            doc: Document

        Returns:

        """
        # match all the IFPs to this news article, record this news article as relevant for all IFPs with simmilarity above threshold

        return DefaultDocumentSelector().select_document(doc)


if __name__ == "__main__":
    master_config = {
        "fields": {
            "type": {
                "type": "string"
            },
            "ifp": {
                "type": "string"
            },
            "news_story": {
                "type": "string"
            },
            "similarity": {
                "type": "number"
            },
            "matched_sentence": {
                "type": "string"
            },
            "date": {
                "type": "string"
            }
        }
    }
    kg_schema = KGSchema(master_config)
    etk = ETK(kg_schema, ["./"])

    # read the news
    news_file = open('/Users/amandeep/Github/etk/examples/ranking_pipeline/resources/new_2018-04-03-first-10000.jl')
    # news_file = open('/Users/amandeep/Github/etk/examples/ranking_pipeline/resources/news_stories_3.jl')
    news_stories = [
        etk.create_document(json.loads(line), url=json.loads(line)['tld'], doc_id=json.loads(line)['doc_id']) for line
        in news_file]
    results = list()
    for news_story in news_stories:
        results.extend(etk.process_ems(news_story))
    o = open('ifp_news_similarity.jl', 'w')
    for result in results:
        o.write(json.dumps(result.value))
        o.write('\n')
