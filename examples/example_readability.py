# This is how we intend to use it 
import sys
import os
import json
import codecs
import fnmatch
from multiprocessing.dummy import Pool as ThreadPool
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


def buildContent(jl, extractors):
    extractors['content_relaxed'] = tk.extract_readability(jl['raw_content'], {'recall_priority': True})
    extractors['content_strict'] = tk.extract_readability(jl['raw_content'], {'recall_priority': False})
    extractors['tables'] = tk.extract_table(jl['raw_content'])


def buildTokensAndData(jl, extractors):
    # for readability and title
    if not config['extractions']:
        raise "No extractions mentioned in config file"

    for extractions in config['extractions']:
        for path in extractions['input']:
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
    if 'tokens' not in match.value.keys():
        match.value['tokens'] = tk.extract_crftokens(match.value['text'])
    if 'simple_tokens' not in match.value.keys():
        match.value['simple_tokens'] = tk.extract_tokens_from_crf(match.value['tokens'])

    data_extractors = {}
    for extractor in extractions['extractors']:
        extractor_info = extractions['extractors'][extractor]

        try:
            function_call = getattr(tk, extractor_info['extractor'])
        except Exception:
            print extractor_info['extractor'], " fucntion not found in etk"
            continue

        # intitalize the data extractor
        data_extractors[extractor] = {'input_type': 'tokens', 'extractor': extractor}

        if extractor_info['input_type'] == 'tokens' and 'name' in extractor_info['config'].keys() and 'ngrams' in extractor_info['config'].keys():
            data_extractors[extractor]['result'] = function_call(match.value['tokens'], name=extractor_info['config']['name'], ngrams=extractor_info['config']['ngrams'])
        elif extractor_info['input_type'] == 'tokens' and 'name' in extractor_info['config'].keys():
            data_extractors[extractor]['result'] = function_call(match.value['simple_tokens'], name=extractor_info['config']['name'])
        elif extractor_info['input_type'] == 'text':
            data_extractors[extractor]['result'] = function_call(match.value['text'])
        else:
            print "No matching call found for input type - ", extractor_info['input_type']

    #  CLEAN THE EMPTY DATA EXTRACTORS
    # for key, value in data_extractors.items():
    #     print key, value
    data_extractors = dict((key, value) for key, value in data_extractors.iteritems() if value['result'])
    if data_extractors:
        match.value['data_extractors'] = data_extractors
    # data_extractors['city'] = tk.extract_using_dictionary(match.value['crf_tokens'], name='cities', ngrams=2)
    # data_extractors['haircolor'] = tk.extract_using_dictionary(match.value['tokens'], name='haircolor')
    # data_extractors['ethnicity'] = tk.extract_using_dictionary(match.value['tokens'], name='ethnicities')
    # data_extractors['eyecolor'] = tk.extract_using_dictionary(match.value['tokens'], name='eyecolor')
    # data_extractors['name'] = tk.extract_using_dictionary(match.value['tokens'], name='names')
    # data_extractors['address'] = tk.extract_address(match.value['text'])

    # for extractor in data_extractors:
    #     if data_extractors[extractor]:
    #         annotateTokenToExtractions(match.value['crf_tokens'], data_extractors[extractor])


def processFile(jl):
    extractors = {}
    # Content extractors
    buildContent(jl, extractors)
    # tokens and data
    buildTokensAndData(jl, extractors)

    jl['extractors'] = extractors
    jl['raw_content'] = '...'

    return jl


def write_output(out, results):
    for i in range(len(results)):
        o.write(json.dumps(results[i]) + '\n')

if __name__ == "__main__":
    input_path = sys.argv[1]
    output_file = sys.argv[2]
    config_file = sys.argv[3]
    if len(sys.argv) == 5:
        threads = sys.argv[4]
    else:
        threads = 5

    config = load_json_file(config_file)

    o = codecs.open(output_file, 'w', 'utf-8')
    i = 1
    files = []
    for jl in jl_file_iterator(input_path):
        pool = ThreadPool(threads)
        files.append(jl)
        if i % threads == 0:
            results = pool.map(processFile, files)
            pool.close()
            pool.join()
            files = []
            write_output(o, results)
        i += 1

    if files:
        results = pool.map(processFile, files)
        pool.close()
        pool.join()
        files = []
        write_output(o, results)
    o.close()
