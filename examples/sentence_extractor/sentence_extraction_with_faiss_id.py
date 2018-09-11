import os
import sys
import json
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from etk.etk import ETK
from etk.document import Document
from etk.etk_module import ETKModule
from etk.extractors.sentence_extractor import SentenceExtractor
from optparse import OptionParser
import datetime as dt
import time

class SentenceSplittingETKModule(ETKModule):
    """
    Abstract class for extraction module
    """

    def __init__(self, etk: ETK):
        ETKModule.__init__(self, etk)
        self.sentence_extractor = SentenceExtractor(name="My sentence splitter")

    def process_document(self, doc: Document):
        """
        Add your code for processing the document
        """

        text_segments = doc.select_segments("lexisnexis.doc_description")
        text = text_segments[0].value
        docid = self.generateUniqueDocId(text)
        for text_segment in text_segments:
            split_sentences_extractions = doc.extract(self.sentence_extractor, text_segment, docId=docid)
            doc.store(split_sentences_extractions, 'split_sentences')
        return list()

    def generateUniqueDocId(self, text: str):
        epochPart = self.getEpochWithMillis()
        pid = '{:>03d}'.format(os.getpid())[-3:]
        assert len(pid) == 3, "PID length incorrect"
        return epochPart + pid

    def getEpochWithMillis(self):
        t = dt.datetime.now()
        epoch = time.mktime(t.timetuple()) + (t.microsecond / 1000000.)
        intpart, decpart = str(epoch).split('.')
        intpart = intpart[-7:] # Just to be safe, using last 10 digits of base
        decpart = '{:>06d}'.format(int(decpart))[:5] # We round off and only first 5 digits of the milliseconds. so precision of 1/10000th

        return ''.join([intpart, decpart])


if __name__ == "__main__":

    etk = ETK(modules=SentenceSplittingETKModule)

    parser = OptionParser(conflict_handler="resolve")
    parser.add_option("-i", "--input_file", action="store",
                      type="string", dest="input_file")
    parser.add_option("-o", "--output_file", action="store",
                      type="string", dest="output_file")
    (c_options, args) = parser.parse_args()

    input_file = c_options.input_file
    output_file = c_options.output_file

    f = open(input_file, mode='r', encoding='utf-8')
    o = open(output_file, mode='w', encoding='utf-8')
    l = open('{}.log'.format(output_file), mode='w', encoding='utf-8')
    print('Starting to process file: {}'.format(input_file))
    count = 0
    sum = 0
    for line in f:
        if count == 10000:
            sum += count
            l.write('Processed {} lines'.format(str(sum)))
            l.write('\n')
            count = 0
        json_x = json.loads(line)
        doc = etk.create_document(json_x)
        doc.doc_id = json_x['doc_id']
        sentences = etk.process_ems(doc)
        for s in sentences:
            o.write(json.dumps(s.value))
            o.write('\n')
        count +=1
