# import os
# import codecs
# path = "/Users/pszekely/Downloads/vipreviews"
# output_file_name = "/Users/pszekely/Downloads/vip_reviews.jl"
# output_file = open(output_file_name, 'w')
#
# onlyfiles = [f for f in listdir(path) if isfile(join(path, f))]
# # print onlyfiles
# index = 0
# for file_name in onlyfiles:
#     file_name_no_ext = os.path.splitext(file_name)[0]
#     input_file = open(join(path, file_name), 'r')
#     raw_content = input_file.read()
#     j = {
#         "raw_content": raw_content,
#         "timestamp": "2017-07-22T00:00:00",
#         "doc_id": '{}_{}'.format(file_name_no_ext, str(index)),
#         "url": "http://www.theeroticreview.com/reviews/showReview.asp?Review=" + file_name_no_ext
#     }
#     index += 1
#     output_file.write(json.dumps(j))
#     output_file.write('\n')
#     input_file.close()
# output_file.close()

import json
import codecs
f = codecs.open('/data/user_data.jl', 'r')
o = codecs.open('/data/user_data_fixed.jl', 'w')
index = 0
for line in f:
    x = json.loads(line)
    x['doc_id'] = '{}_{}'.format(x['doc_id'], str(index))
    index += 1
    o.write(json.dumps(x))
    o.write('\n')
o.close()
f.close()
