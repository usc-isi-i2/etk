import time
import json
import codecs
import sys
import multiprocessing as mp, os
import core
from optparse import OptionParser
""" Process code begins here """


class ParallelPtocess(object):
    """ Class to run the process in parallel """

    def __init__(self, input_path, output_path, config, processes=0):
        self.input = input_path
        self.output = output_path
        self.processes = processes
        self.core = core.Core(extraction_config=json.load(codecs.open(config, 'r')))

    @staticmethod
    def output_write(output_path):
        return codecs.open(output_path, 'w')

    @staticmethod
    def chunk_file(file_name, size=1024 * 1024):
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
                document = json.loads(line)
                try:
                    document = self.core.process(document, create_knowledge_graph=True)
                except Exception as e:
                    print "Failed - ", e
                with open(self.output, "a") as file_write:
                    file_write.write(json.dumps(document) + '\n')
                # print "Processing chunk - ", str(chunk_start), " File - ", str(i)

    def run_parallel(self, processes=0):
        self.processes = self.processes or processes or mp.cpu_count()
        pool = mp.Pool(self.processes)
        jobs = []
        for chunk_start, chunk_size in self.chunk_file(self.input):
            jobs.append(pool.apply_async(work, (self, chunk_start, chunk_size)))
        for job in jobs:
            job.get()
        pool.close()

    def run_serial(self):
        output = codecs.open(self.output, 'w')
        index = 1
        for line in codecs.open(self.input):
            print 'processing line:', index
            start_time_doc = time.time()
            jl = json.loads(line)
            # try:
            result = self.core.process(jl, create_knowledge_graph=True)
            output.write(json.dumps(result) + '\n')
            # except Exception as e:
            #         print "Failed - %s : %s" % (e, jl['url'])
            time_taken_doc = time.time() - start_time_doc
            # print "Took", str(time_taken_doc), " seconds"
            if time_taken_doc > 5:
                print 'long time for %s - %s' % (jl['url'], jl['tld'])
            index += 1
        output.close()


def work(instance, start, size):
    instance.process_wrapper(start, size)


def usage():
    return """\
Usage: python run_core.py [args]
-i, --input <input_doc>                   Input file
-o, --output <output_doc>                 Output file
-c, --config <config>                     Config file

Optional
-t, --thread <processes_count>            Serial(default=0)
                                          Run Parallel(>0)
    """


if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-i", "--input", action="store", type="string", dest="inputPath")
    parser.add_option("-o", "--output", action="store", type="string", dest="outputPath")
    parser.add_option("-c", "--config", action="store", type="string", dest="configPath")
    parser.add_option("-t", "--thread", action="store", type="string", dest="threadCount", default=0)

    (c_options, args) = parser.parse_args()

    if not (c_options.inputPath and c_options.outputPath and c_options.configPath):
        print usage()
        sys.exit()
    try:
        start_time = time.time()
        inst = ParallelPtocess(c_options.inputPath, c_options.outputPath, c_options.configPath)
        if c_options.threadCount:
            print "Processing parallel with " + c_options.threadCount + " processes"
            inst.run_parallel(int(c_options.threadCount))
        else:
            print "processing serially"
            inst.run_serial()
        print('The script took {0} second !'.format(time.time() - start_time))
    except Exception as e:
        print e


