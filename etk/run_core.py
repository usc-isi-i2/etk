import time

import json
import codecs
import os
import sys
import multiprocessing as mp, os

# input_path = '/Users/amandeep/Github/etk/etk/test_data/cdr_two.jl'
# output_path = '/Users/amandeep/Github/etk/etk/test_data/out_two.jl'
# config_path = '/Users/amandeep/Github/etk/etk/resources/extraction_config.json'

input_path = '/Users/Muthu/Desktop/cdr_1k.jl'
output_path = '/Users/Muthu/Desktop/cc'
config_path = './resources/extraction_config.json'
extraction_config = json.load(codecs.open(config_path, 'r'))

# print sys.path
sys.path.append('./core.py')
# sys.path.append('/Users/amandeep/anaconda2/envs/memexeval2017/lib/python2.7/site-packages')
from core import Core

start_time = time.time()
from pprint import pprint

""" Process code begins here """
o = codecs.open(output_path, 'w')
c = Core(extraction_config)
time_taken = time.time() - start_time


class ParallelPtocess(object):
    """ Class to run the process in parallel """

    def __init__(self, input_path, output, config, processes=0):
        self.input = input_path
        self.output = self.output_write(output)
        self.processes = processes
        self.core = Core(extraction_config)

    @staticmethod
    def output_write(output_path):
        return codecs.open(output_path, 'w')


    def chunk_file(self, file_name, size=1024 * 1024):
        """ Splitting data into chunks for parallel processing
        :param file_name - name of the file to split
        :param size - size of file to split
        """
        file_end = os.path.getsize(file_name)
        with open(file_name, 'r') as f:
            chunk_end = f.tell()
            while True:
                chunk_start = chunk_end
                f.seek(size, 1)
                f.readline()
                chunk_end = f.tell()
                yield chunk_start, chunk_end - chunk_start
                if chunk_end > file_end:
                    break

    def process_wrapper(self, chunk_start, chunk_size):
        with open(self.input) as f:
            f.seek(chunk_start)
            lines = f.read(chunk_size).splitlines()
            for i, line in enumerate(lines):
                start_time_doc = time.time()
                document = json.loads(line)
                try:
                    document = self.core.process(document)
                except Exception as e:
                    print "Failed - ", e
                o.write(json.dumps(document) + '\n')
                time_taken_doc = time.time() - start_time_doc
                print "Processing chunk - ", str(chunk_start), " File - ", str(i), str(time_taken_doc), " seconds"

    def run_parallel(self, processes=0):
        self.processes = self.processes or processes or mp.cpu_count()
        pool = mp.Pool(self.processes)
        jobs = []
        for chunk_start, chunk_size in self.chunk_file(self.input):
            jobs.append(pool.apply_async(work, (self, chunk_start, chunk_size)))
        for job in jobs:
            job.get()
        pool.close()
        self.output.close()

    def run_serial(self):
        for line in codecs.open(self.input):
            start_time_doc = time.time()
            jl = json.loads(line)
            o.write(json.dumps(c.process(jl)) + '\n')
            time_taken_doc = time.time() - start_time_doc
            print "Took", str(time_taken_doc), " seconds"
        self.output.close()

def work(instance, start, size):
    instance.process_wrapper(start, size)

def usage():
    return """\
Usage: python run_core.py [args]
<input_doc>                  Input file
<output_doc>                 Output file
<config>                     Config file
Optional <processes_count>   Run Parallel
    """


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print usage()
        sys.exit()
    try:
        start_time = time.time()
        inst = ParallelPtocess(sys.argv[1], sys.argv[2], sys.argv[3])
        if len(sys.argv) == 5:
            print "Processing parallel with " + sys.argv[4] + " processes"
            inst.run_parallel(int(sys.argv[4]))
        else:
            print "processing serially"
            inst.run_serial()
        print('The script took {0} second !'.format(time.time() - start_time))
    except Exception as e:
        print e


