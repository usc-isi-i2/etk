from optparse import OptionParser
from pyspark import SparkContext, SparkConf, StorageLevel
import json
import slim_down_etk_output

__author__ = 'amandeep'

if __name__ == '__main__':
    compression = "org.apache.hadoop.io.compress.GzipCodec"

    parser = OptionParser()

    (c_options, args) = parser.parse_args()
    input_path = args[0]
    output_path = args[1]

    sc = SparkContext(appName="SLIM-ETK-OUTPUT")
    conf = SparkConf()

    input_rdd = sc.sequenceFile(input_path)

    output_rdd = input_rdd.mapValues(json.loads).mapValues(slim_down_etk_output.slim_etk_out)

    output_rdd = output_rdd.filter(lambda x: x[1] is not None).mapValues(json.dumps)
    output_rdd.saveAsSequenceFile(output_path, compressionCodecClass=compression)
