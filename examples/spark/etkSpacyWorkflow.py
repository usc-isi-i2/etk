#!/usr/bin/env python

from pyspark import SparkContext, StorageLevel
from sys import argv
from etk.core import Core
import  json
import codecs

def extract_from_spacy(json_doc_iter):
        c = Core(load_spacy=True)
        for json_doc_tuple in json_doc_iter:
           json_doc = json_doc_tuple[1]
           text = json_doc['text']
           extractions = ''
           extractions  = c.extract_spacy(text)
           person_extractions = set()
           org_extractions = set()
           if "PERSON" in extractions:
           	for p in extractions["PERSON"]:
                  person_extractions.add(p.strip())
           if "ORG" in extractions:
                for o in extractions["ORG"]:
                  org_extractions.add(o.strip())

           json_doc['extractions'] = {}
           json_doc['extractions']['person'] = {'extractor':'spacy', 'values': list(person_extractions)}
           json_doc['extractions']['organization'] = {'extractor':'spacy', 'values': list(org_extractions)}
	   yield json_doc_tuple[0], json_doc

if __name__ == "__main__":
    sc = SparkContext(appName="ETK-Spacy-Workflow")
    inputFilename = argv[1]
    #outputFilename = argv[2]
    
    rdd = sc.sequenceFile(inputFilename).mapValues(lambda x: json.loads(x))
    outputRDD = rdd.mapPartitions(extract_from_spacy)
    for x in outputRDD.take(5):
      print "----------------------------------------------"
      print json.dumps(x[1]['extractions'])

    #outputRDD.mapValues(lambda x: json.dumps(x)).saveAsSequenceFile(outputFilename)
