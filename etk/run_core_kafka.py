import time
import json
import codecs
import sys
import os
import multiprocessing as mp
import core
from argparse import ArgumentParser
from kafka import KafkaProducer, KafkaConsumer
from digsandpaper.elasticsearch_indexing.index_knowledge_graph import index_knowledge_graph_fields
import traceback


def run_serial(input, output, core, prefix='', kafka_server=None, kafka_topic=None):
    # ignore file output if kafka is set
    if kafka_server is None:
        output = codecs.open(output, 'w')
    else:
        kafka_server = kafka_server.split(',')
        kafka_producer = KafkaProducer(
            bootstrap_servers=kafka_server,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    index = 1
    for line in codecs.open(input):
        print prefix, 'processing line number:', index
        start_time_doc = time.time()
        jl = json.loads(line)
        result = core.process(jl, create_knowledge_graph=True)
        if result:
            if kafka_producer is None:
                output.write(json.dumps(result) + '\n')
            else:
                r= kafka_producer.send(kafka_topic, result)
                r.get(timeout=60) # wait till sent
            time_taken_doc = time.time() - start_time_doc
            if time_taken_doc > 5:
                print prefix, "Took", str(time_taken_doc), " seconds"
        else:
            print 'Failed line number:', index
        index += 1

    if kafka_producer is None:
        output.close()

def run_serial_cdrs(core, consumer, producer, producer_topic, indexing=False):
    # high level api will handle batch thing
    # will exit once timeout
    for msg in consumer:
        cdr = msg.value
        if 'doc_id' not in cdr:
            cdr['doc_id'] = cdr.get('_id', cdr.get('document_id', ''))
        if len(cdr['doc_id']) == 0:
            print 'invalid cdr: unknown doc_id'
        print 'processing', cdr['doc_id']
        try:
            result = core.process(cdr, create_knowledge_graph=True)
            if not result:
                raise Exception('run core error')
            # indexing
            if indexing:
                result = index_knowledge_graph_fields(result)
            # dumping result
            if result:
                r = producer.send(producer_topic, result)
                r.get(timeout=60)  # wait till sent
            print 'done'
        except Exception as e:
            # print e
            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
            print ''.join(lines)
            print 'failed at', cdr['doc_id']


def run_parallel_3(input_path, output_path, config_path, processes, kafka_server, kafka_topic):
    if not os.path.exists(output_path) or not os.path.isdir(output_path) :
        raise Exception('temp path is invalid')
    # if len(os.listdir(temp_path)) != 0:
    #     raise Exception('temp path is not empty')
    if processes < 1:
        raise Exception('invalid process number')

    # split input file into chunks
    print 'splitting input file...'
    with codecs.open(input_path, 'r') as input:
        input_chunk_file_handlers = [
            codecs.open(os.path.join(output_path, 'input_chunk_{}.json'.format(i)), 'w') for i in xrange(processes)]
        idx = 0
        for line in input:
            if line == '\n':
                continue
            input_chunk_file_handlers[idx].write(line)
            idx = (idx + 1) % processes
        for f in input_chunk_file_handlers:
            f.close()

    # create processes
    print 'creating workers...'
    print '-------------------'
    process_handlers = []
    for i in xrange(processes):
        input_chunk_path = os.path.join(output_path, 'input_chunk_{}.json'.format(i))
        output_chunk_path = os.path.join(output_path, 'output_chunk_{}.json'.format(i))
        p = mp.Process(target=run_parallel_worker,
                   args=(i, input_chunk_path, output_chunk_path, config_path,
                         kafka_server, kafka_topic))
        process_handlers.append(p)

    # start processes
    for p in process_handlers:
        p.start()

    # wait till finish
    for p in process_handlers:
        p.join()

    print '-------------------'


def run_parallel_worker(worker_id, input_chunk_path, output_chunk_path, config_path, kafka_server, kafka_topic):
    print 'start worker #{}'.format(worker_id)
    c = core.Core(json.load(codecs.open(config_path, 'r')))
    run_serial(input_chunk_path, output_chunk_path, c,
               prefix='worker #{}:'.format(worker_id), kafka_server=kafka_server, kafka_topic=kafka_topic)
    print 'worker #{} finished'.format(worker_id)


def usage():
    return """\
Usage: python run_core.py [args]
-i, --input <input_doc>                   Input file
-o, --output <output_doc>                 Output file (serial), output / temp directory path (multi-processing)
-c, --config <config>                     Config file

Optional
-m, --multiprocessing-enabled
-t, --multiprocessing-processes <processes>
                                         
--kafka-input-server <host:port,...>
--kafka-input-topic <topic_name>
--kafka-input-group-id <group_id>
--kafka-input-session-timeout <ms>
--kafka-output-server <host:port,...>
--kafka-output-topic <topic_name>

--indexing
    """

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-i", "--input", action="store", type=str, dest="inputPath")
    parser.add_argument("-o", "--output", action="store", type=str, dest="outputPath")
    parser.add_argument("-c", "--config", action="store", type=str, dest="configPath")

    parser.add_argument("-m", "--multiprocessing-enabled", action="store_true", dest="mpEnabled")
    parser.add_argument("-t", "--multiprocessing-processes", action="store",
                        type=int, dest="mpProcesses", default=mp.cpu_count())

    parser.add_argument("--kafka-input-server", action="store", type=str, dest="kafkaInputServer")
    parser.add_argument("--kafka-input-topic", action="store", type=str, dest="kafkaInputTopic")
    parser.add_argument("--kafka-input-group-id", action="store", type=str, dest="kafkaInputGroupId")
    parser.add_argument("--kafka-input-session-timeout", action="store", type=int,
                        dest="kafkaInputSessionTimeout", default=60*60*1000) # default to 1 hour
    parser.add_argument("--kafka-output-server", action="store", type=str, dest="kafkaOutputServer")
    parser.add_argument("--kafka-output-topic", action="store", type=str, dest="kafkaOutputTopic")
    parser.add_argument("--kafka-input-args", action="store", type=str, dest="kafkaInputArgs")
    parser.add_argument("--kafka-output-args", action="store", type=str, dest="kafkaOutputArgs")
    parser.add_argument("--indexing", action="store_true", dest="indexing")

    c_options, args = parser.parse_known_args()

    if not c_options.configPath and \
       not (c_options.inputPath or
                (c_options.kafkaInputServer and c_options.kafkaInputTopic and c_options.kafkaInputGroupId)) and \
       not (c_options.outputPath or (c_options.kafkaOutputServer or c_options.kafkaOutputTopic)):
        usage()
        sys.exit()

    # kafka input
    if c_options.kafkaInputServer is not None:
        try:
            # parse input and output args
            input_args = json.loads(c_options.kafkaInputArgs) if c_options.kafkaInputArgs else {}
            output_args = json.loads(c_options.kafkaOutputArgs) if c_options.kafkaOutputArgs else {}

            # print 'input:'
            # print c_options.kafkaInputServer.split(',')
            # print c_options.kafkaInputGroupId
            # print c_options.kafkaInputSessionTimeout
            # print c_options.kafkaInputTopic
            # print input_args

            kafka_input_server = c_options.kafkaInputServer.split(',')
            consumer = KafkaConsumer(
                bootstrap_servers=kafka_input_server,
                group_id=c_options.kafkaInputGroupId,
                    consumer_timeout_ms=c_options.kafkaInputSessionTimeout,
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                **input_args
            )
            consumer.subscribe([c_options.kafkaInputTopic])
            c = core.Core(json.load(codecs.open(c_options.configPath, 'r')))
            kafka_output_server = c_options.kafkaOutputServer.split(',')
            producer = KafkaProducer(
                bootstrap_servers=kafka_output_server,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                **output_args
            )
            run_serial_cdrs(c, consumer, producer, c_options.kafkaOutputTopic, indexing=c_options.indexing)

        except Exception as e:
            # print e
            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
            print ''.join(lines)

    # file input
    else:
        try:
            start_time = time.time()
            if c_options.mpEnabled and c_options.mpProcesses > 1:
                print "processing parallelly"
                run_parallel_3(
                    input_path=c_options.inputPath,
                    output_path=c_options.outputPath,
                    config_path=c_options.configPath,
                    processes=c_options.mpProcesses,
                    kafka_server=c_options.kafkaServer,
                    kafka_topic=c_options.kafkaTopic)
            else:
                print "processing serially"
                c = core.Core(json.load(codecs.open(c_options.configPath, 'r')))
                run_serial(c_options.inputPath, c_options.outputPath, c,
                           kafka_server=c_options.kafkaServer, kafka_topic=c_options.kafkaTopic)
            print('The script took {0} second !'.format(time.time() - start_time))

        except Exception as e:
            print e


