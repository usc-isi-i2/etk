import time
import json
import codecs
import sys
import multiprocessing as mp, os
import core
from optparse import OptionParser
# from concurrent import futures
from pathos.multiprocessing import ProcessingPool
from pathos import multiprocessing as mpp
import multiprocessing as mp
import pathos
# from pathos.helpers
import gzip
""" Process code begins here """


def output_write(output_path):
    return codecs.open(output_path, 'w+')


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


def process_wrapper(core, input, chunk_start, chunk_size, queue):
    results = []
    with open(input) as f:
        f.seek(chunk_start)
        lines = f.read(chunk_size).splitlines()
        for i, line in enumerate(lines):
            document = json.loads(line)
            try:
                document = core.process(document, create_knowledge_graph=True)
            except Exception as e:
                print "Failed - ", e

            # queue.put(json.dumps(document))
            # print "Processing chunk - ", str(chunk_start), " File - ", str(i)


def listener(queue, output):
    f = open(output, 'wb')
    while 1:
        message = queue.get()
        if message == 'kill':
            print "Done writing to file......."
            break
        f.write(message + '\n')
        f.flush()
    f.close()


def run_parallel(input, output, core, processes=0):
    processes = processes or mp.cpu_count()
    processes += 2 # for writing

    manager = mp.Manager()
    queue = manager.Queue()
    pool = mp.Pool(processes)

    # put listener to work first
    watcher = pool.apply_async(listener, (queue, output))

    jobs = []
    
    for chunk_start, chunk_size in chunk_file(input):
        jobs.append(pool.apply_async(process_wrapper, (core, input, chunk_start, chunk_size, queue)))
    for job in jobs:
        job.get()
    queue.put('kill')
    pool.close()


def run_serial(input, output, core):
    output = codecs.open(output, 'w')
    index = 1
    for line in codecs.open(input):
        print 'processing line number:', index
        start_time_doc = time.time()
        jl = json.loads(line)
        result = core.process(jl, create_knowledge_graph=True)
        output.write(json.dumps(result) + '\n')
        time_taken_doc = time.time() - start_time_doc
        if time_taken_doc > 5:
            print "Took", str(time_taken_doc), " seconds"
        index += 1
    output.close()


def process_one(x):
    # output = "output-%d.gz" % pathos.helpers.mp.current_process().getPid()
    output = c_options.outputPath + "/output-%d.jl" % mp.current_process().pid
    with codecs.open(output, "a+") as out:
        out.write('%s\n' % json.dumps(c.process(x)))

def run_parallel_2(input_path, output_path, core, processes=0):
    lines = codecs.open(input_path, 'r').readlines()
    inputs = list()
    # pool = ProcessingPool(16)
    pool = mpp.Pool(8)
    for line in lines:
        inputs.append(json.loads(line))
    # pool = .ProcessPoolExecutor(max_workers=8)
    # results = list(pool.map(process_one, inputs))
    pool.map(process_one, inputs)

    # output_f = codecs.open(output_path, 'w')
    # for result in results:
    #     output_f.write(json.dumps(result))
    #     output_f.write('\n')


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
        c = core.Core(json.load(codecs.open(c_options.configPath, 'r')))
        start_time = time.time()
        if c_options.threadCount:
            print "Processing parallel with " + c_options.threadCount + " processes"
            # run_parallel(c_options.inputPath, c_options.outputPath, c,  int(c_options.threadCount))
            run_parallel_2(c_options.inputPath, c_options.outputPath, c, int(c_options.threadCount))
        else:
            print "processing serially"
            run_serial(c_options.inputPath, c_options.outputPath, c)
        print('The script took {0} second !'.format(time.time() - start_time))
    except Exception as e:
        print e


