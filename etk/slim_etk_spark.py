from optparse import OptionParser
from pyspark import SparkContext, SparkConf, StorageLevel
import json

__author__ = 'amandeep'


def slim_etk_out(doc):
    doc.pop('crawl_data', None)
    doc.pop('extractions', None)
    doc.pop('raw_content', None)
    doc.pop('response_headers', None)
    doc.pop('content_extraction', None)
    doc.pop('content_type', None)
    doc.pop('extracted_metadata', None)
    doc.pop('extracted_text', None)
    doc.pop('@execution_profile', None)

    if 'objects' in doc:
        objects = doc['objects']
        if not isinstance(objects, list):
            objects = [objects]
        for object in objects:
            object.pop('response_headers', None)
        doc['objects'] = objects

    doc['knowledge_graph'] = clean_kg(doc['knowledge_graph'])
    return doc


def clean_kg(knowledge_graph):
    knowledge_graph.pop('states_usa_codes', None)
    knowledge_graph.pop('city_name', None)
    knowledge_graph.pop('populated_places', None)
    knowledge_graph.pop('location', None)

    for key in knowledge_graph.keys():
        extractions = knowledge_graph[key]
        for extraction in extractions:
            provs = extraction['provenance']
            if not isinstance(provs, list):
                provs = [provs]
            for prov in provs:
                prov.pop('confidence', None)
                if 'source' in prov and 'context' in prov['source']:
                    context = prov['source']['context']
                    context.pop('input', None)
                    context.pop('start', None)
                    context.pop('end', None)
                    prov['source']['context'] = context
            extraction['provenance'] = provs
        knowledge_graph[key] = extractions
    return knowledge_graph


if __name__ == '__main__':
    compression = "org.apache.hadoop.io.compress.GzipCodec"

    parser = OptionParser()

    (c_options, args) = parser.parse_args()
    input_path = args[0]
    output_path = args[1]

    sc = SparkContext(appName="SLIM-ETK-OUTPUT")
    conf = SparkConf()

    input_rdd = sc.sequenceFile(input_path)

    output_rdd = input_rdd.mapValues(json.loads).mapValues(slim_etk_out)

    output_rdd = output_rdd.filter(lambda x: x[1] is not None).mapValues(json.dumps)
    output_rdd.saveAsSequenceFile(output_path, compressionCodecClass=compression)
