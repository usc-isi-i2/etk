from optparse import OptionParser
from pyspark import SparkContext, SparkConf
import requests
# from elasticsearch import Elasticsearch
import json

__author__ = 'amandeep'

image_query = """{"_source": ["identifier", "url"],
                            "query": {
                                "term": {
                                    "isImagePartOf.uri": {

                                    }
                                }
                            }
                            }"""

ad_uri_prefix = 'http://dig.isi.edu/ht/data/webpage/'

source_index = 'dig-4'
source_doc_type = 'image'
# source_es = Elasticsearch(['http://10.1.94.68:9200'])
es_url = 'http://10.1.94.68:9200'


def convert_image_to_cdr3(images_list):
    all_images = list()
    for image_obj in images_list:
        image = image_obj['_source']
        i_obj = dict()
        i_obj['identifier'] = image['identifier']
        i_obj['obj_stored_url'] = image['url']
        all_images.append(i_obj)
    return all_images


def search(query):
    return json.loads(requests.post('{}/{}/{}/_search'.format(es_url, source_index, source_doc_type), json=query).text)


def get_images_from_es(ad):
    q = json.loads(image_query)
    q["query"]["term"]["isImagePartOf.uri"]["value"] = '{}{}'.format(ad_uri_prefix, ad['doc_id'])
    r = None
    # try 10 times in case of time out error
    for i in range(0,10):
        try:
            r = search(q)
            break
        except:
            continue
    if r:
        all_images = convert_image_to_cdr3(r['hits']['hits'])
        if len(all_images) > 0:
            ad['objects'] = all_images
        else:
            print 'No images for ad: {}'.format(ad['doc_id'])
    return ad

if __name__ == '__main__':
    compression = "org.apache.hadoop.io.compress.GzipCodec"

    parser = OptionParser()
    parser.add_option("-p", "--partitions", action="store",
                      type="int", dest="partitions", default=0)
    (c_options, args) = parser.parse_args()
    input_path = args[0]
    output_path = args[1]

    partitions = c_options.partitions

    sc = SparkContext(appName="CONVERT-To-CDR3")
    conf = SparkConf()

    if partitions == 0:
        input_rdd = sc.sequenceFile(input_path)
    else:
        input_rdd = sc.sequenceFile(input_path).partitionBy(partitions)
    output_rdd = input_rdd.mapValues(json.loads).mapValues(get_images_from_es).mapValues(json.dumps)

    output_rdd.saveAsSequenceFile(output_path, compressionCodecClass=compression)
    output_rdd = None
    output_rdd = sc.sequenceFile(output_path).mapValues(json.loads)

    print output_rdd.filter(lambda x: 'objects' in x[1] and len(x[1]['objects']) > 0).count()
    print output_rdd.filter(lambda x: 'objects' not in x[1]).count()
