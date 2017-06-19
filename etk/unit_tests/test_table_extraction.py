import unittest
import sys
import os
sys.path.append('../../')
from etk.core import Core
import json
import codecs

# infile = open('/Users/majid/DIG/etk/etk/unit_tests/ground_truth/table.jl')
infile = open(sys.argv[1])
e_config = json.load(open('./resources/extraction_config_table.json'))
c = Core(extraction_config=e_config)
outfile = open(sys.argv[2], 'w')
counter = 1
for line in infile:
    # if counter < 14584:
    #     counter += 1
    #     continue
    print('processing line {0}'.format(counter))
    doc = json.loads(line)
    # print(doc)
    # try:
    r = c.process(doc, create_knowledge_graph=True)
    # except Exception, e:
    #     print(doc)
    #     continue
    outfile.write(json.dumps(r).encode('utf-8') + '\n')
    counter += 1
    # self.assertTrue("content_extraction" in r)
    # self.assertTrue("table" in r["content_extraction"])
    # ex = json.loads(json.JSONEncoder().encode(r["content_extraction"]["table"]))
    # self.assertEqual(ex, self.table_ex)