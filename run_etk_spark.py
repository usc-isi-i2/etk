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

def remove_extra_fields(x):
    if 'content_extraction' in x:
        ce = x['content_extraction']
        for key in ce.keys():
            t = ce[key]
            if 'simple_tokens_original_case' in t:
                t.pop('simple_tokens_original_case')
            if 'simple_tokens' in t:
                t.pop('simple_tokens')
            if 'data_extraction' in t:
                t.pop('data_extraction')
            ce[key] = t
        x['content_extraction'] = ce
    return x


if __name__ == '__main__':
    compression = "org.apache.hadoop.io.compress.GzipCodec"

    parser = OptionParser()
    parser.add_option("-p", "--partitions", action="store",
                      type="int", dest="partitions", default=1000)
    (c_options, args) = parser.parse_args()
    input_path = args[0]
    output_path = args[1]
    extraction_config_path = args[2]

    partitions = c_options.partitions

    sc = SparkContext(appName="ETK-Extractions")
    conf = SparkConf()
    extraction_config = json.load(codecs.open(extraction_config_path))
    c = Core(extraction_config=extraction_config)

    input_rdd = sc.sequenceFile(input_path)#.partitionBy(partitions)

    output_rdd = input_rdd.mapValues(json.loads).filter(lambda x: remove_if_no_html(x[1])).mapValues(add_doc_id)\
        .mapValues(lambda x: c.process(x, create_knowledge_graph=True))

    output_rdd = output_rdd.filter(lambda x: x[1] is not None).mapValues(remove_extra_fields).mapValues(json.dumps)
    output_rdd.saveAsSequenceFile(output_path, compressionCodecClass=compression)

    # print sc.sequenceFile(input_path).count()
    # print sc.sequenceFile(output_path).count()

