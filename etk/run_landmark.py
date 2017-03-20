import json
import codecs
from optparse import OptionParser
from data_extractors import landmark_extraction


if __name__ == "__main__":
    parser = OptionParser()
    (c_options, args) = parser.parse_args()
    input_file = args[0]
    output_file = args[1]
    landmark_rules_file = args[2]

    landmark_rules = json.load(codecs.open(landmark_rules_file, 'r'))

    input = codecs.open(input_file, 'r')
    output = codecs.open(output_file, 'w')
    i = 1
    for line in input:
        print 'processing line # :', str(i)
        jl = json.loads(line)
        result = landmark_extraction.landmark_extractor(jl, landmark_rules)
        output.write(json.dumps(result) + '\n')
        i += 1
    output.close()