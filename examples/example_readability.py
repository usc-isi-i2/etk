# This is how we intend to use it 
import sys
import os
import json
import codecs
import fnmatch
sys.path.insert(1, os.path.join(sys.path[0], '..'))
import time
import etk


tk = etk.init()


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

if __name__ == "__main__":

    input_path = sys.argv[1]
    for jl in jl_file_iterator(input_path):
        print "Readability Extractor with recall"
        print tk.extract_readability(jl['raw_content'], {'recall_priority': True})
        print "Readability Extractor without recall"
        print tk.extract_readability(jl['raw_content'], {'recall_priority': False})
