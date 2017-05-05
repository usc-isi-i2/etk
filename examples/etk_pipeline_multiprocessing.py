"""
Class for running etk pipeline in parallel using a config file
"""

import sys, os
import json
import codecs
import fnmatch
from multiprocessing import Process
import multiprocessing as mp
from jsonpath_rw import parse
import time
# TODO: Remove in future once etk published
sys.path.insert(1, os.path.join(sys.path[0], '..'))
import etk


class ETKParallelProcess:
    def __init__(self, configuration_file, input_file, output_file):
        self.config = self.load_json_file(configuration_file)
        self.input_file = input_file
        self.output_file = self.open_file(output_file)
        self.record_count = 0
        # library
        self.tk = ''
        self.__intialize_libraries()

    def open_file(self, file_name):
        return codecs.open(file_name, 'w', 'utf-8')

    def load_json_file(self, file_name):
        rules = json.load(codecs.open(file_name, 'r', 'utf-8'))
        return rules

    def __intialize_libraries(self):
        self.tk = etk.init()
        self.tk.load_dictionaries()

    def process_wrapper(self, chunkStart, chunkSize):
        with open(self.input_file) as f:
            f.seek(chunkStart)
            lines = f.read(chunkSize).splitlines()
            for line in lines:
                document = json.loads(line)
                self.process_file(document)

    # Splitting data into chunks for parallel processing
    def chunkify(self, filename, size=1024*1024):
        fileEnd = os.path.getsize(filename)
        with open(filename,'r') as f:
            chunkEnd = f.tell()
            while True:
                chunkStart = chunkEnd
                f.seek(size,1)
                f.readline()
                chunkEnd = f.tell()
                yield chunkStart, chunkEnd - chunkStart
                if chunkEnd > fileEnd:
                    break

    def process_file(self, jl):
        extractors = {}
        # Content extractors
        self.build_content(jl, extractors)
        # tokens and data
        self.build_tokens_and_data(jl, extractors)

        jl['extractors'] = extractors
        jl['raw_content'] = '...'
        self.output_file.write(json.dumps(jl) + '\n')

    def build_content(self, jl, extractors):
        extractors['content_relaxed'] = self.tk.extract_readability(jl['raw_content'], {'recall_priority': True})
        extractors['content_strict'] = self.tk.extract_readability(jl['raw_content'], {'recall_priority': False})
        extractors['tables'] = self.tk.extract_table(jl['raw_content'])


    def build_tokens_and_data(self, jl, extractors):
        # for readability and title
        if not self.config['extractions']:
            raise "No extractions mentioned in config file"

        for extractions in self.config['extractions']:
            for path in extractions['input']:
                jsonpath_expr = parse(path)
                try:
                    matches = jsonpath_expr.find(extractors)
                except Exception:
                    print "No matches for ", path
                    matches = []
                for match in matches:
                    self.process_data_match(match, extractions)

    @staticmethod
    def annotate_token_to_extractions(tokens, extractions):
        for extractor, extraction in extractions.iteritems():
            input_type = extraction['input_type']
            if 'text' == input_type:
                # build text tokens
                continue

            data = extraction['result']
            for values in data:
                start = values['context']['start']
                end = values['context']['end']
                offset = 0
                for i in range(start, end):
                    if 'semantic_type' not in tokens[i].keys():
                        tokens[i]['semantic_type'] = []
                    temp = {'type': extractor, 'offset': offset}
                    if offset == 0:
                        temp['length'] = end - start
                    tokens[i]['semantic_type'].append(temp)
                    offset += 1

    @staticmethod
    def annotate_text_to_extractions(tokens, extractions):
        for extractor, extraction in extractions.iteritems():
            input_type = extraction['input_type']
            if 'text' == input_type:
                # build text tokens
                continue

            data = extraction['result']
            for values in data:
                start = values['context']['start']
                end = values['context']['end']
                offset = 0
                for i in range(start, end):
                    if 'semantic_type' not in tokens[i].keys():
                        tokens[i]['semantic_type'] = []
                    temp = {'type': extractor, 'offset': offset}
                    if offset == 0:
                        temp['length'] = end - start
                    tokens[i]['semantic_type'].append(temp)
                    offset += 1

    def process_data_match(self, match, extractions):
        if 'tokens' not in match.value.keys():
            match.value['tokens'] = self.tk.extract_crftokens(match.value['text'])
        if 'simple_tokens' not in match.value.keys():
            match.value['simple_tokens'] = self.tk.extract_tokens_from_crf(match.value['tokens'])

        data_extractors = {}
        for extractor in extractions['extractors']:
            extractor_info = extractions['extractors'][extractor]

            try:
                function_call = getattr(self.tk, extractor_info['extractor'])
            except Exception:
                print extractor_info['extractor'], " fucntion not found in etk"
                continue
            try:
                # initialize the data extractor
                data_extractors[extractor] = {'input_type': 'tokens', 'extractor': extractor}

                if extractor_info['input_type'] == 'tokens' and 'name' in extractor_info['config'].keys() and 'ngrams' in extractor_info['config'].keys():
                    data_extractors[extractor]['result'] = function_call(match.value['tokens'], name=extractor_info['config']['name'], ngrams=extractor_info['config']['ngrams'])
                elif extractor_info['input_type'] == 'tokens' and 'name' in extractor_info['config'].keys():
                    data_extractors[extractor]['result'] = function_call(match.value['simple_tokens'], name=extractor_info['config']['name'])
                elif extractor_info['input_type'] == 'text':
                    data_extractors[extractor]['result'] = function_call(match.value['text'])
                else:
                    print "No matching call found for input type - ", extractor_info['input_type']
            except Exception as e:
                data_extractors[extractor]['result'] = ''
                print extractor, " crashed - ", e

        #  CLEAN THE EMPTY DATA EXTRACTORS
        data_extractors = dict((key, value) for key, value in data_extractors.iteritems() if value['result'])
        if data_extractors:
            self.annotate_token_to_extractions(match.value['tokens'], data_extractors)
            self.annotate_text_to_extractions(match.value['tokens'], data_extractors)
            match.value['data_extractors'] = data_extractors

    def run_parallel(self, processes=4):
        processes = int(processes)
        pool = mp.Pool(processes)
        try:
            pool = mp.Pool(processes)
            jobs = []
            # run for chunks of files
            for chunkStart,chunkSize in self.chunkify(input_path):
                jobs.append(pool.apply_async(self.process_wrapper,(chunkStart,chunkSize)))
            for job in jobs:
                job.get()
            pool.close()
        except Exception as e:
            print e

    def jl_file_iterator(self, file):
        with codecs.open(file, 'r', 'utf-8') as f:
            for line in f:
                document = json.loads(line)
                yield document

    def run_serial(self):
        for jl in self.jl_file_iterator(input_path):
            self.process_file(jl)


if __name__ == "__main__":
    # read arguments
    input_path = sys.argv[1]
    output_file = sys.argv[2]
    config_file = sys.argv[3]

    ep = ETKParallelProcess(config_file, input_path, output_file)
    startTime = time.time()

    if len(sys.argv) == 5:
        ep.run_parallel(sys.argv[4])
    else:
        ep.run_serial()
    print('The script took {0} second !'.format(time.time() - startTime))


