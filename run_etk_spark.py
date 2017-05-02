from optparse import OptionParser
from pyspark import SparkContext, SparkConf, StorageLevel
import json
from etk.core import Core
import codecs
__author__ = 'amandeep'


needed_fields = ['raw_content', 'content_extraction', 'knowledge_graph', 'url', 'timestamp', 'tld']


def remove_junk(x):
    new_x = dict()
    if 'doc_id' in x:
        new_x['doc_id'] = x['doc_id']
    elif '_id' in x:
        new_x['doc_id'] = x['_id']

    for field in needed_fields:
        if field in x:
            new_x[field] = x[field]
    return new_x

if __name__ == '__main__':
    compression = "org.apache.hadoop.io.compress.GzipCodec"

    parser = OptionParser()
    (c_options, args) = parser.parse_args()
    input_path = args[0]
    output_path = args[1]
    extraction_config_path = args[2]

    sc = SparkContext(appName="ETK-Extractions")
    conf = SparkConf()
    extraction_config = json.load(codecs.open(extraction_config_path))
    c = Core(extraction_config=extraction_config)
    input_rdd = sc.sequenceFile(input_path).partitionBy(100)
    input_rdd = input_rdd.mapValues(json.loads).mapValues(
        lambda x: c.process(x, create_knowledge_graph=True)).mapvalues(remove_junk).mapValues(json.dumps)
    input_rdd.saveAsSequenceFile(output_path, compressionCodecClass=compression)

