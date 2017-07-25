import json
import codecs
import gzip
import os

if __name__ == '__main__':

    o = codecs.open('test_data/eccie.net/filtered.jl', 'w')
    csi = 0
    cri = 0
    eti = 0
    path = 'test_data/eccie.net/'
    for f_f in os.listdir(path):
        if f_f.endswith('gz'):
            f = gzip.open(path + '/' + f_f, 'r')
            for line in f:
                x = json.loads(line)
                x.pop('knowledge_graph', None)
                x.pop('indexed', None)
                x.pop('content_extraction', None)
                o.write(json.dumps(x))
                o.write('\n')

    o.close()

    print 'Relevant pages in content_strict: {}'.format(str(csi))
    # print 'Relevant pages in content_relaxed: {}'.format(str(cri))
    # print 'Extracted texts: {}'.format(str(eti))