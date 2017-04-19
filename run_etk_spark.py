from optparse import OptionParser
from pyspark import SparkContext, SparkConf, StorageLevel
import json
from etk.core import Core
import codecs
__author__ = 'amandeep'


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
    input_rdd = input_rdd.mapValues(json.loads).mapValues(c.process).mapValues(json.dumps)
    input_rdd.saveAsSequenceFile(output_path, compressionCodecClass=compression)

