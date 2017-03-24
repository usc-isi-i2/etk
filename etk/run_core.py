import json
import codecs
input_path = '/Users/amandeep/Github/etk/etk/test_data/cdr_two.jl'
output_path = '/Users/amandeep/Github/etk/etk/test_data/out_two.jl'
config_path = '/Users/amandeep/Github/etk/etk/resources/extraction_config.json'
extraction_config = json.load(codecs.open(config_path, 'r'))
import sys
# print sys.path
sys.path.append('/Users/amandeep/Github/etk/etk')
sys.path.append('/Users/amandeep/anaconda2/envs/memexeval2017/lib/python2.7/site-packages')
from core import Core
from pprint import pprint

o = codecs.open(output_path, 'w')
c = Core(extraction_config)
for line in codecs.open(input_path):
    jl = json.loads(line)
    o.write(json.dumps(c.process(jl)) + '\n')
o.close()