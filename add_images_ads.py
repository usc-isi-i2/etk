from optparse import OptionParser
import time
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import codecs
from datetime import datetime


class Janitor(object):
    def __init__(self, ads_no_images_path, log_path):
        self.ad_uri_prefix = 'http://dig.isi.edu/ht/data/webpage/'
        self.image_query = {"_source": ["identifier", "url"],
                            "query": {
                                "term": {
                                    "isImagePartOf.uri": {

                                    }
                                }
                            }
                            }
        self.o_file = codecs.open(ads_no_images_path, 'a+')
        self.log_file = codecs.open(log_path, 'a+')
        self.source_index = 'dig-4'
        self.source_doc_type = 'image'
        self.source_es = Elasticsearch(['http://10.1.94.68:9200'])
        self.dest_index = 'dig-etk-search-10'
        self.dest_doc_type = 'ads'
        self.dest_es = Elasticsearch(['http://10.1.94.103:9201'])
        self.total_docs_processed = 0

    def get_images_from_es(self, ads):
        new_ads = list()
        for ad in ads:
            q = self.image_query
            q["query"]["term"]["isImagePartOf.uri"]["value"] = '{}{}'.format(self.ad_uri_prefix, ad['_id'])
            r = self.source_es.search(index=self.source_index, doc_type=self.source_doc_type, body=q)
            all_images = self.convert_image_to_kg(r['hits']['hits'])
            if len(all_images) > 0:
                source = ad['_source']
                if 'knowledge_graph' not in source:
                    source['knowledge_graph'] = dict()
                source['knowledge_graph']['all_images'] = all_images
                if 'dig_version' not in source:
                    source['dig_version'] = 1.0
                else:
                    source['dig_version'] += 1.0
                ad['_source'] = source
                new_ads.append(ad)
            else:
                print 'No images for ad: {}'.format(ad['_id'])
                # self.o_file.write(ad['_id'])
                # self.o_file.write('\n')
        self.bulk_upload(new_ads)
        updated_docs = len(new_ads)
        self.total_docs_processed += updated_docs
        self.log_file.write('{}\n'.format(datetime.now().isoformat()))
        self.log_file.write('Documents updated in this batch: {}\n'.format(str(updated_docs)))
        self.log_file.write('Total documents processed till date: {}\n'.format(str(self.total_docs_processed)))

    @staticmethod
    def convert_image_to_kg(images_list):
        all_images = list()
        for image_obj in images_list:
            image = image_obj['_source']
            kg_object = dict()
            kg_object['confidence'] = 0.5
            kg_object['key'] = image['identifier']
            kg_object['value'] = image['url']
            all_images.append(kg_object)
        return all_images

    def get_ads(self, batch_size=10):
        # TODO PROCESS BACKPAGE first
        q = {
            "query": {
                "filtered": {
                    "query": {"match_all": {}},
                    "filter": {
                        "and": {
                            "filters": [
                                {
                                    "term": {
                                        "knowledge_graph.website.key": "backpage.com"
                                    }
                                },
                                {
                                    "not": {
                                        "filter": {
                                            "exists": {
                                                "field": "dig_version"
                                            }
                                        }
                                    }
                                }
                            ]
                        }
                    }
                }
            }
        }
        page = self.dest_es.search(index=self.dest_index, doc_type=self.dest_doc_type, scroll='2m',
                                   search_type='scan', size=batch_size, body=q)
        sid = page['_scroll_id']
        scroll_size = page['hits']['total']

        # Start scrolling
        while (scroll_size > 0):
            page = self.dest_es.scroll(scroll_id=sid, scroll='2m')
            # Update the scroll ID
            sid = page['_scroll_id']
            scroll_size = len(page['hits']['hits'])
            print "scroll size: " + str(scroll_size)

            self.get_images_from_es(page['hits']['hits'])

    def bulk_upload(self, ads):
        actions = list()
        for ad in ads:
            action = {
                "_op_type": "update",
                "_index": ad["_index"],
                "_type": ad["_type"],
                "_id": ad['_id'],
                "doc": ad['_source']
            }
            actions.append(action)
        helpers.bulk(self.dest_es, actions)


if __name__ == '__main__':
    parser = OptionParser()
    (c_options, args) = parser.parse_args()
    ads_no_images_path = args[0]
    log_path = args[1]
    j = Janitor(ads_no_images_path, log_path)
    start_time = time.time()
    for i in range(0,1000):
        try:
            j.get_ads()
        except:
            j.log_file.write('Failed attempt: {}\n'.format(i))
            pass
    j.log_file.write('The script took {0} second !'.format(time.time() - start_time))

