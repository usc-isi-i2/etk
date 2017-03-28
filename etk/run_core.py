import time

import json
import codecs
input_path = '/Users/amandeep/Github/etk/etk/test_data/cdr_two.jl'
output_path = '/Users/amandeep/Github/etk/etk/test_data/out_two.jl'
config_path = '/Users/amandeep/Github/etk/etk/resources/extraction_config.json'
extraction_config = json.load(codecs.open(config_path, 'r'))
import sys
# print sys.path

sys.path.append('/Users/amandeep/Github/etk/etk')

# sys.path.append('/Users/amandeep/anaconda2/envs/memexeval2017/lib/python2.7/site-packages')
from core import Core
start_time = time.time()
from pprint import pprint

o = codecs.open(output_path, 'w')

c = Core(extraction_config)
time_taken = time.time() - start_time
# print 'time taken to initialise %s' % time_taken
for line in codecs.open(input_path):
    start_time = time.time()
    jl = json.loads(line)

    o.write(json.dumps(c.process(jl)) + '\n')
    time_taken = time.time() - start_time
    print 'time taken to process document %s' % time_taken
o.close()