import os, sys, json
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from etk.etk import ETK
from etk.knowledge_graph import KGSchema
from etk.document_selector import DefaultDocumentSelector
from etk.doc_retrieve_processor import DocRetrieveProcessor
from heapq import heappush
import spacy
from etk.extractors.date_extractor import DateExtractor
import re


# TODO: modify the input news and ifp_titles path here
# filename = './resources/new_2018-04-03-first-10000.jl'
# query_title = './resources/ifps_titles_test.txt'

def main():
    filename = sys.argv[1]
    query_title = sys.argv[2]
    ranking_criteria = sys.argv[3]
    top_k = sys.argv[4]

    if ranking_criteria not in ('TITLE', 'SENTENCE'):
        print('Wrong mode! Please check the input argument!')
        return

    master_config = {
        "fields": {
            "developer": {
                "type": "string"
            },
            "student_developer": {
                "type": "string"
            },
            "spacy_name": {
                "type": "string"
            },
            "date": {
                "type": "date"
            }
        }
    }
    kg_schema = KGSchema(master_config)
    etk = ETK(kg_schema, ["./extraction_modules/"])
    nlp = spacy.load('en_core_web_lg')

    date_extractor = DateExtractor(etk=etk)

    queries = dict()
    queries_ent_map = dict()

    with open(query_title) as f:
        for line in f:
            orig_ifp_title = line
            # remove date information from query term
            res = date_extractor.extract(text=line)
            start, end = float('inf'), -1
            for i in res:
                start = min(start, i.provenance['start_char'])
                end = max(end, i.provenance['end_char'])
            # delete date from query term
            if len(res) != 0:
                line = line[:start] + line[end+1:]

            queries[orig_ifp_title] = line
            queries_ent_map[line] = list()
            # extract entities from query term
            doc = nlp(line)
            for ent in doc.ents:
                queries_ent_map[line].append(re.escape(ent.text.strip()))
            # remove empty entities
            queries_ent_map[line] = list(filter(bool, queries_ent_map[line]))

    # the list of selected docs for given query term
    query_docs_mapping = dict()

    docs = list()
    with open(filename) as f:
        for line in f:
            json_obj = json.loads(line)
            docs.append(etk.create_document(json_obj))

    ds = DefaultDocumentSelector()

    for orig_query, proc_query in queries.items():
        content_regex = queries_ent_map[proc_query]
        query_docs_mapping[proc_query] = list()
        for doc in docs:
            if len(content_regex) == 0 \
                    or ds.select_document(document=doc,
                              json_paths=['$.lexisnexis.doc_description'],
                              json_paths_regex=content_regex):
                query_docs_mapping[proc_query].append(doc)

    # TODO: pass ifp_id in
    for orig_query, proc_query in queries.items():
        # print(len(query_docs_mapping[proc_query]))
        dr_processor = DocRetrieveProcessor(etk=etk, ifp_id="1233", ifp_title=proc_query, orig_ifp_title=orig_query)
        heap = list()
        for doc in query_docs_mapping[proc_query]:
            processed_doc = dict()

            if ranking_criteria == 'SENTENCE':
                processed_doc = dr_processor.process_by_sentence(doc=doc, threshold=0).cdr_document
            elif ranking_criteria == 'TITLE':
                processed_doc = dr_processor.process_by_title(doc=doc, threshold=0).cdr_document

            if len(heap) < top_k:
                heappush(heap, (processed_doc['similarity'], processed_doc['date'], processed_doc))
            else:
                if processed_doc['similarity'] > heap[0][0]:
                    heappush(heap, (processed_doc['similarity'], processed_doc['date'], processed_doc))

        heap.sort(reverse=True)

        output_filename = './resources/output/'+orig_ifp_title+"_result.jl"

        with open(output_filename, 'a+b') as f:
            for item in heap:
                print(item[0])
                jl_str = json.dumps(item[2]) + '\n'
                f.write(jl_str.encode())


if __name__ == "__main__":
  main()
