from optparse import OptionParser
from pyspark import SparkContext, SparkConf, StorageLevel
import json
from etk.core import Core
import codecs
__author__ = 'amandeep'


needed_fields = ['raw_content', 'content_extraction', 'knowledge_graph', 'url', 'timestamp', 'tld']


def add_doc_id(x):
    if '_id' in x:
        x['doc_id'] = x['_id']
    return x


def remove_if_no_html(x):
    if 'raw_content' not in x:
        return False
    rc = x['raw_content']
    if not rc:
        return False
    if rc.strip() == '':
        return False
    return True

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
    input_rdd = sc.sequenceFile(input_path)#.partitionBy(1000)
    print input_rdd.first()
    input_rdd = input_rdd.mapValues(json.loads).filter(lambda x: remove_if_no_html(x[1])).mapValues(add_doc_id)\
        .mapValues(lambda x: c.process(x, create_knowledge_graph=True))
    input_rdd = input_rdd.filter(lambda x: x[1] is not None).mapValues(json.dumps)
    input_rdd.saveAsSequenceFile(output_path, compressionCodecClass=compression)

