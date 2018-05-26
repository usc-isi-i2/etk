import spacy
import json
import pandas as pd
import re
from etk.document import Document


class DocRetrieveProcessor(object):

    def __init__(self, etk, ifp_id: str, ifp_title: str, orig_ifp_title: str) -> None:
        self.etk = etk
        self.ifp_id = ifp_id
        self.ifp_title = ifp_title
        self.nlp = spacy.load('en_core_web_lg')
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
        # Note: ^ this is returning the cosSim of the AVERAGED .vectors in the list of tokens being compared
        max_score = max(scores)
        max_score_idx = scores.index(max_score)
        max_score_sentence = sentences[max_score_idx]

        if max_score > threshold:
            output_cdr_doc = {
                'type': 'News/IPF Relevance',
                'date': timestamp,
                'ifp': self.orig_ifp_title,
                'news_story': doc_id,
                'similarity': max_score,
                'matched_sentence': max_score_sentence
            }
        else:
            output_cdr_doc = {
                'type': None,
                'date': None,
                'ifp': None,
                'news_story': None,
                'similarity': 0.0,
                'matched_sentence': None
            }

        return self.etk.create_document(output_cdr_doc)

    def process_by_title(self, doc: Document, threshold: float) -> Document:
        json_obj = doc.cdr_document

        # secondary key
        timestamp = json_obj['@timestamp']
        date = pd.to_datetime(timestamp, infer_datetime_format=True)
        doc_id = json_obj['doc_id']

        title = json_obj['lexisnexis']['doc_title']
        title_token = self.nlp(title)
        similarity = self.query_tokens.similarity(title_token)
        # Note: ^ this is returning the cosSim of the AVERAGED .vectors in the list of tokens being compared

        if similarity > threshold:
            output_obj = {
                'type': 'News/IPF Relevance',
                'date': timestamp,
                'ifp': self.orig_ifp_title,
                'news_story': doc_id,
                'title': title,
                'similarity': similarity
            }
        else:
            output_obj = {
                'type': None,
                'date': None,
                'ifp': None,
                'news_story': None,
                'title': None,
                'similarity': 0.0
            }

        return self.etk.create_document(output_obj)