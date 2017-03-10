# This is how we intend to use it 
import sys
import os
import json
import codecs
import fnmatch
from jsonpath_rw import parse, jsonpath
sys.path.insert(1, os.path.join(sys.path[0], '..'))
import time
import etk


tk = etk.init()
tk.load_dictionaries()


def load_json_file(file_name):
    rules = json.load(codecs.open(file_name, 'r', 'utf-8'))
    return rules


def jl_file_iterator(file):
    with codecs.open(file, 'r', 'utf-8') as f:
        for line in f:
            document = json.loads(line)
            yield document


def jl_path_iterator(file_path):
    abs_file_path = os.path.abspath(file_path)
    if os.path.isdir(abs_file_path):
        for file in os.listdir(abs_file_path):
            if fnmatch.fnmatch(file, '*.jl'):
                yield os.path.join(abs_file_path, file)
    else:
        yield abs_file_path


def buildContent(jl, doc):
    extractors['content_relaxed'] = tk.extract_readability(jl['raw_content'], {'recall_priority': True})
    extractors['content_strict'] = tk.extract_readability(jl['raw_content'], {'recall_priority': False})
    extractors['tables'] = tk.extract_table(jl['raw_content'])


def buildTokensAndData(jl, extractors, config):
    # for readability and title
    if not config['extractions']:
        raise "No extractions mentioned in config file"

    for extractions in config['extractions']:
        for path in extractions['input']:
            print path
            jsonpath_expr = parse(path)
            try:
                matches = jsonpath_expr.find(extractors)
            except Exception:
                print "No matches for ", path
                matches = []
            for match in matches:
                processDataMatch(match, extractions)

    # # for tables
    # if 'tables' in extractors.keys() and extractors['tables']:
    #     jsonpath_expr = parse('tables[*].rows[*].cells[*].text.`parent`')
    #     matches = jsonpath_expr.find(extractors)
    #     for match in matches:
    #         # pass
    #         processDataMatch(match)


def annotateTokenToExtractions(tokens, extractions):
    for extractor, extractions in extractions.iteritems():
        if extractor in ['phone']:
            " ignoring phone annotation.."
            continue
        for extraction in extractions:
            input_type = extraction['input_type']
            if 'text' in input_type:
                # build text tokens
                pass

            if 'tokens' not in input_type:
                print "ignoring ", extractor, " as tokens not dependant.."
                continue
            data = extraction['result']
            for values in data:
                start = values['context']['start']
                end = values['context']['end']
                offset = 0
                for i in range(start, end):
                    if 'semantic_type' not in tokens[i].keys():
                        tokens[i]['semantic_type'] = []
                    temp = {}
                    temp['type'] = extractor
                    temp['offset'] = offset
                    if offset == 0:
                        temp['length'] = end - start
                    tokens[i]['semantic_type'].append(temp)
                    offset += 1
    return tokens


def processDataMatch(match, extractions):
    if 'crf_tokens' not in match.value.keys():
        match.value['crf_tokens'] = tk.extract_crftokens(match.value['text'])
    if 'tokens' not in match.value.keys():
        match.value['tokens'] = tk.extract_tokens_from_crf(match.value['crf_tokens'])

    data_extractors = {}
    data_extractors['city'] = tk.extract_using_dictionary(match.value['crf_tokens'], name='cities', ngrams=2)
    data_extractors['haircolor'] = tk.extract_using_dictionary(match.value['tokens'], name='haircolor')
    data_extractors['ethnicity'] = tk.extract_using_dictionary(match.value['tokens'], name='ethnicities')
    data_extractors['eyecolor'] = tk.extract_using_dictionary(match.value['tokens'], name='eyecolor')
    data_extractors['name'] = tk.extract_using_dictionary(match.value['tokens'], name='names')
    data_extractors['address'] = tk.extract_address(match.value['text'])

    # for extractor in data_extractors:
    #     if data_extractors[extractor]:
    #         annotateTokenToExtractions(match.value['crf_tokens'], data_extractors[extractor])

    match.value['data_extractors'] = data_extractors

if __name__ == "__main__":
    input_path = sys.argv[1]
    output_file = sys.argv[2]
    config_file = sys.argv[3]
    config = load_json_file(config_file)

    o = codecs.open(output_file, 'w', 'utf-8')
    for jl in jl_file_iterator(input_path):
        extractors = {}
        # Content extractors
        buildContent(jl, extractors)
        # tokens and data
        buildTokensAndData(jl, extractors, config)

        jl['extractors'] = extractors
        jl['raw_content'] = '...'
        o.write(json.dumps(jl) + '\n')

    o.close()
