import copy


class ReadabilityExtractor():

    def __init__(self):
        self.renamed_input_fields = 'html'
        self.recall_priority = True
        self.html_partial = False
        self.metadata = {
            'extractor': "readability",
            'recall_priority': str(self.recall_priority),
            'html_partial': str(self.html_partial)
        }

    def get_recall_priority(self):
        self.recall_priority

    def set_recall_priority(self, recall_priority):
        self.recall_priority = recall_priority
        self.metadata['recall_priority'] = str(recall_priority)
        return self

    def get_html_partial(self):
        self.recall_priority

    def set_html_partial(self, html_partial):
        self.html_partial = html_partial
        self.metadata['html_partial'] = str(html_partial)
        return self

    def extract(self, html_content):
        from readability.readability import Document
        from bs4 import BeautifulSoup
        try:
            if html_content:
                readable = Document(html_content, recallPriority=self.recall_priority).summary(html_partial=self.html_partial)
                cleantext = BeautifulSoup(readable.encode('utf-8'), 'lxml').strings
                readability_text = ' '.join(cleantext)
                return readability_text
            else:
                return ''
        except Exception, e:
            print 'Error in extracting readability %s' % e
            return ''

    def get_metadata(self):
        return copy.copy(self.metadata)

    def set_metadata(self, metadata):
        self.metadata = metadata
        return self

    def get_renamed_input_fields(self):
        return self.renamed_input_fields
