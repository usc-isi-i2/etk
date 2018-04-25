import spacy
import json
import pandas as pd
import re
from etk.document import Document


class DocRetrieveProcessor(object):

    def __init__(self, etk, ifp_id: str, ifp_title: str) -> None:
        self.etk = etk
        self.ifp_id = ifp_id
        self.ifp_title = ifp_title
        self.nlp = spacy.load('en_core_web_lg')
        self.query_tokens = self.nlp(self.ifp_title)

        sentence_tokenizer = '(?<!\w\.\w)(?<![A-Z][a-z]\.)(?<=\.|\?)\s+(?=[A-Z])'
        self.sentence_tokenizer_pattern = re.compile(sentence_tokenizer)

    # process document sentence by sentence
    def process(self, doc: Document, threshold: float) -> Document:
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
                'type': 'News/IPF Relevance',
                'date': timestamp,
                'ifp': self.ifp_title,
                'news_story': doc_id,
                'similarity': max_score,
                'matched_sentence': max_score_sentence
            }

        return self.etk.create_document(output_cdr_doc)
