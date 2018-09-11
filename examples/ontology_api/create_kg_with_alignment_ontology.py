import os, sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from etk.etk import ETK
from etk.knowledge_graph import KGSchema
from etk.etk_module import ETKModule
from etk.ontology_api import Ontology, rdf_generation
from rdflib.namespace import RDF
import re
from time import mktime, gmtime
from datetime import datetime, timedelta

class AlignmentHelper():
    @staticmethod
    def parse_date(date, date_format=None):

        """Convert a date to ISO8601 date format
        input format: YYYY-MM-DD HH:MM:SS GMT (works less reliably for other TZs)
        or            YYYY-MM-DD HH:MM:SS.0
        or            YYYY-MM-DD
        or            epoch (13 digit, indicating ms)
        or            epoch (10 digit, indicating sec)
        output format: iso8601"""
        date = date.strip()

        if date_format:
            try:
                if date_format.find('%Z') != -1:
                    date_format = date_format.replace('%Z', '')
                    match_object = re.search('(([-+])(\d{2})(\d{2}))', date)
                    if match_object is None:
                        match_object = re.search('(([-+])(\d{2}):(\d{2}))', date)
                    tz = match_object.groups()

                    dt = datetime.strptime(date.replace(tz[0], ''), date_format)
                    delta = timedelta(hours=int(tz[2]), minutes=int(tz[3]))
                    if tz[1] == '-': delta = delta * -1
                    dt = dt + delta

                    return dt

                return datetime.strptime(date, date_format)
            except Exception:
                pass

        try:
            return datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
        except:
            pass

        try:
            # Friday, October 2, 2015 1:35 AM
            return datetime.strptime(date, "%A, %B %d, %Y %I:%M %p")
        except:
            pass

        try:
            # Friday, 2 October 2015, 18:23
            return datetime.strptime(date, "%A, %d %B %Y, %H:%M")
        except:
            pass

        try:
            # Thu October 01st, 2015
            return datetime.strptime(date, "%a %B %dst, %Y")
        except:
            pass

        try:
            # Thu October 02nd, 2015
            return datetime.strptime(date, "%a %B %dnd, %Y")
        except:
            pass

        try:
            # Thu October 03rd, 2015
            return datetime.strptime(date, "%a %B %drd, %Y")
        except:
            pass

        try:
            # Thu October 04th, 2015
            return datetime.strptime(date, "%a %B %dth, %Y")
        except:
            pass

        try:
            return datetime.strptime(date, "%Y-%m-%d %H:%M:%S %Z")
        except Exception:
            pass

        try:
            return datetime.strptime(date, "%A, %b %d, %Y")
        except Exception:
            pass

        try:
            return datetime.strptime(date, "%Y-%m-%d %H:%M:%S.0")
        except:
            pass

        try:
            return datetime.strptime(date, "%Y-%m-%d")
        except:
            pass

        try:
            return datetime.strptime(date, "%b %d, %Y")
        except:
            pass

        try:
            return datetime.strptime(date, "%B %d, %Y")
        except:
            pass

        try:
            return datetime.strptime(date, "%B %d, %Y %I:%M %p")
        except:
            pass

        try:
            return datetime.strptime(date, "%b %d, %Y at %I:%M %p")
        except:
            pass

        try:
            return datetime.strptime(date, "%m-%d-%Y")
        except:
            pass

        try:
            return datetime.strptime(date, "%Y-%m-%dT%H:%M:%S")
        except:
            pass

        try:
            return datetime.strptime(date, "%Y-%m-%dT%H:%M:%SZ")
        except:
            pass

        try:
            return datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ")
        except:
            pass

        try:
            return datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%f+00:00")
        except:
            pass

        try:
            date = int(date)
            if 1000000000000 < date < 9999999999999:
                # 13 digit epoch
                return datetime.fromtimestamp(mktime(gmtime(date / 1000)))
        except:
            pass

        try:
            date = int(date)
            if 1000000000 < date < 9999999999:
                # 10 digit epoch
                return datetime.fromtimestamp(mktime(gmtime(date)))
        except:
            pass
        # If all else fails, return empty
        return None

    @staticmethod
    def iso8601date(date, date_format=None):
        date_obj = AlignmentHelper.parse_date(date, date_format)
        if date_obj is not None:
            return date_obj.isoformat()
        return ''

    @staticmethod
    def uri_from_fields(prefix, *fields):
        """Construct a URI out of the fields, concatenating them after removing offensive characters.
        When all the fields are empty, return empty"""
        string = '_'.join(AlignmentHelper.alpha_numeric(f.strip().lower(), '') for f in fields)

        if len(string) == len(fields) - 1:
            return ''

        return prefix + string

    @staticmethod
    def alpha_numeric(x, replacement_string=' '):
        """Replace consecutive non-alphanumeric bya replacement_string"""
        return re.sub('[^A-Za-z0-9]+', replacement_string, x)


class SpaceawareAlignmentModule(ETKModule):
    def __init__(self, etk):
        ETKModule.__init__(self, etk)
        self.uri_prefix = "http://spaceaware.isi.edu/data"

    def process_document(self, doc):
        data =  doc.cdr_document.get('data', {})
        aligned_doc = self.align_document(data)
        return self.__create_document(aligned_doc)

    def align_document(self, doc):
        '''
        :param doc: Input document
        :return: Aligned document
        This method is called to do alignment with Ontology, including transformations for the input data
        '''
        pass

    def add_prov_attributes(self, doc, attributes):
        contains_data_attributes = False
        for key in doc:
            value = doc[key]
            if isinstance(value, dict):
                value = self.add_prov_attributes(value, attributes)
                doc[key] = value
            elif key != "@id" and key != "a" and key != "@type" and key != "uri":
                contains_data_attributes = True

        if contains_data_attributes:
            for attr_key in attributes:
                doc[attr_key] = attributes[attr_key]
        return doc

    def __create_document(self, data):
        type_ = data.get(RDF.type.toPython(), list())
        type_.extend(data.get('@type', list()))
        doc = self.etk.create_document(dict(), doc_id=data['@id'], type_=type_)
        docs = [doc]
        for key in data:
            if key in {'@id', '@type', RDF.type.toPython()}:
                continue
            field_name = doc.kg.context_resolve(key)
            if data[key] is not None:
                value = data[key]
                if isinstance(value, dict):
                    # at most nest 1 level, no need to worry what it returns
                    subdocs = self.__create_document(value)
                    for subdoc in subdocs:
                        docs.append(subdoc)
                        doc.kg.add_value(field_name, subdoc.doc_id)
                else:
                    doc.kg.add_value(field_name, value)
        # for doc in docs:
        #     print("Create document returned:", doc.kg.value)
        return docs


class SeesatETKModule(SpaceawareAlignmentModule):
    def __init__(self, etk):
        SpaceawareAlignmentModule.__init__(self, etk)

    def document_selector(self, doc):
        return doc.cdr_document.get("source_name") == "seesat"

    def align_document(self, doc):
        # print(doc)
        doc_data = doc #["data"]
        doc_json_content = doc_data["json_content"]
        msg_content = doc_json_content["content"]

        url_cleaned = self.uri_prefix + AlignmentHelper.uri_from_fields("/email/", msg_content['url'])
        ssplit = re.split('\(|<|>', msg_content['sender'])
        sender_email = None
        sender_name = None
        for s in ssplit:
            if '@' in s:
                sender_email = s.strip()
            elif len(s) > 2:
                sender_name = s.strip()
        sender_uri = None
        if sender_name is not None:
            sender_uri = self.uri_prefix + AlignmentHelper.uri_from_fields("/person/", sender_name)
        elif sender_uri is not None:
            sender_uri = self.uri_prefix + AlignmentHelper.uri_from_fields("/person/", sender_email)

        next_thread_uri = None
        replyToMessage_uri = None
        if 'nxt' in msg_content:
            next_thread_uri = self.uri_prefix + AlignmentHelper.uri_from_fields("/email/", msg_content['nxt'])
        if 'rep_to' in msg_content:
            replyToMessage_uri = self.uri_prefix + AlignmentHelper.uri_from_fields("/email/", msg_content['rep_to'])

        aligned_doc = {
            "@type": ["http://schema.org/EmailMessage"],
            "@id": url_cleaned,
            "http://schema.org/about": {
                "@type": ["http://schema.org/Thing"],
                "@id": url_cleaned + "/subject",
                "http://schema.org/description": msg_content["subject"]
            },
            "http://schema.org/sender": {
                "@type": ["http://schema.org/Person"],
                "@id": sender_uri,
                "http://schema.org/name": sender_name,
                "http://schema.org/email": sender_email
            },
            "http://schema.org/recipient": {
                '@id': self.uri_prefix + AlignmentHelper.uri_from_fields('/organization/', msg_content['recip']),
                '@type':["http://schema.org/Organization"],
                'http://schema.org/email': msg_content['recip']
            },
            "http://schema.org/url": msg_content["url"],
            "http://schema.org/text": msg_content["body"],
            "http://schema.org/nextInThread": next_thread_uri,
            "http://schema.org/replyToMessage": replyToMessage_uri
        }

        prov_attributes = {
            "http://www.w3.org/ns/prov#dateRecorded": AlignmentHelper.parse_date(doc_data["timestamp"]),
            "http://www.w3.org/ns/prov#publisher": doc_data["source_name"],
            "http://www.w3.org/ns/prov#source": doc_data["_id"]
        }

        return self.add_prov_attributes(aligned_doc, prov_attributes)


if __name__ == "__main__":
    import json

    sample_input = [
        {
            "source_name": "seesat",
            "timestamp": "1533489319",
            "content_type": "application/json",
            "url": "http://spaceaware.isi.edu/input/seesatFFFFFFFFADB351B0",
            "version": "2.0",
            "json_content": {
                "date": "2018-08-04T20:22:13.433928",
                "content": {
                    "recip": "seesat-l@satobs.org",
                    "body": "31601 07 025A   4641 F 20180731194122700 17 25 2049770+052582 28\n31601 07 025A   4641 F 20180731194235560 17 25 1759480+062688 18\n39232 13 043A   4641 F 20180731200018670 17 25 1907680+060837 18\n39232 13 043A   4641 F 20180731200035380 17 25 1907920+165100 18\n90113 16 535A   4641 F 20180731201701910 17 25 1530000-210045 58\n90113 16 535A   4641 F 20180731201812530 17 25 1613280-213233 28 \n90113 16 535A   4641 F 20180731202202060 17 25 1808475-211519 37\n\nAlberto Rango, Bari,  4641   41°.1067N   016°.9008E  35 mt \n\n\n\n_______________________________________________\nSeesat-l mailing list\nhttp://mailman.satobs.org/mailman/listinfo/seesat-l",
                    "sender": "Alberto Rango",
                    "url": "http://www.satobs.org/seesat/Aug-2018/0000.html",
                    "dateReceived": "2018-08-01T16:05:12",
                    "@context": {
                        "@vocab": "schema.org"
                    },
                    "subject": "4641   SATOBS   31 JUL 2018, Ofeq 7, USA 245, ISON 32000."
                },
                "_id": "084162a5510055a7d66daeaf0965158f492a3680e0991b38c9d0db5e6f563af9"
            },
            "_id": "seesat/FFFFFFFFADB351B0"
        },
        {
            "source_name": "seesat",
            "timestamp": "1533403420",
            "content_type": "application/json",
            "url": "http://spaceaware.isi.edu/input/seesatFFFFFFFF874801E5",
            "version": "2.0",
            "_id": "seesatFFFFFFFF874801E5",
            "json_content": {
                "date": "2018-08-03T11:05:42.007270",
                "content": {
                    "recip": "seesat-l@satobs.org",
                    "body": "40887 15 044AÂ  7782 G 20180406154433100 57 25 0922184+053033 57\n40887 15 044AÂ  7782 G 20180406154453100 57 25 0922375+053011 57\n40887 15 044AÂ  7782 G 20180406154459500 57 25 0922442+053003 57\n40887 15 044AÂ  7782 G 20180406154519500 57 25 0922442+053003 57\n40887 15 044AÂ  7782 G 20180406154551500 57 25 0923357+052906 57\n26635 00 080AÂ  7782 G 20180406144922700 57 25 1105584+121004 57\n26635 00 080AÂ  7782 G 20180406144948700 57 25 1106050+120954 57\n26635 00 080AÂ  7782 G 20180406145008700 57 25 1106247+120921 57\n26635 00 080AÂ  7782 G 20180406145014600 57 25 1106311+120911 57\n26635 00 080AÂ  7782 G 20180406145034600 57 25 1106507+120838 57\n23648 95 038CÂ  7782 G 20180406145034600 57 25 1105542+121246 57\n23648 95 038CÂ  7782 G 20180406145042100 57 25 1106023+121221 57\n23648 95 038CÂ  7782 G 20180406145102100 57 25 1106219+121101 57\n40208 14 055AÂ  7782 G 20180406144713200 57 25 0920212+050847 57\n40208 14 055AÂ  7782 G 20180406144733200 57 25 0920408+050847 57\n40208 14 055AÂ  7782 G 20180406144739400 57 25 0920475+050848 57\n40208 14 055AÂ  7782 G 20180406144759400 57 25 0921073+050848 57\n40208 14 055AÂ  7782 G 20180406144831400 57 25 0921394+050849 57\n\nStrays:\n38779N 40269N 39216N 39508 41588 41105N 12897N 42698 41869N 41552N 11648N 41103N 37606 39017 42695N\n\nBrad Young Visual:\nBright:20 x 80 Celestron binocularsÂ \nDim:22\" f/4.2 UC Obsession\nCOSPAR 8336 =TULSA1 +36.139208,-95.983429 660ft, 201m\nCOSPAR 8335 =TULSA2 +35.8311 Â -96.1411 1083ft, 330m\nRemote Imaging:\n7779 32.92 -105.528 7000 Mayhill, New Mexico USA\n7778 -31.2733 149.0644 3400 Siding Spring, NSW, Australia\n7777 38.165653 -2.326735 5150 Nerpio, Spain\n7780 37.07 -119.4 4610 Auberry CA USA\n7782 -32.008 -116.135 984 Perth, WA, Australia\nNumbers above and methods explained at: http://www.satobs.org/seesat/Jan-2015/0074.htmlÂ \n\n_______________________________________________\nSeesat-l mailing list\nhttp://mailman.satobs.org/mailman/listinfo/seesat-l",
                    "sender": "Brad Young",
                    "nxt": "http://www.satobs.org/seesat/Apr-2018/0042.html",
                    "url": "http://www.satobs.org/seesat/Apr-2018/0040.html",
                    "rep_to": "http://www.satobs.org/seesat/Apr-2018/00422.html",
                    "dateReceived": "2018-04-07T22:13:00",
                    "@context": {
                        "@vocab": "schema.org"
                    },
                    "subject": "BY R Perth 040618"
                },
                "_id": "716bd382c716514339e72d4452c18cddfc5aae515596b29db3a7cb846bece9e5"
            }
        }
    ]

    ontology_content = ""

    dir_path = os.path.dirname(os.path.abspath(__file__))
    input_turtle = dir_path + "/../../ontologies/"
    with open(input_turtle + '/schema.ttl') as s:
        ontology_content += s.read()
    with open(input_turtle + '/prov.ttl') as s:
        ontology_content += s.read()
    with open(input_turtle + '/schema-ext.ttl') as s:
        ontology_content += s.read()
    #print("Ontology::\n", ontology_content)

    ontology = Ontology(ontology_content, validation=False, include_undefined_class=True, quiet=True, expanded_jsonld=False)
    kg_schema = KGSchema(ontology.merge_with_master_config(dict()))
    etk = ETK(modules=SeesatETKModule, kg_schema=kg_schema, ontology=ontology,
              generate_json_ld=True, output_kg_only=True)

    # sample_input_jsons = json.loads(sample_input, encoding='utf-8', strict=False)
    for sample_input_json in sample_input:
        input_data = {'doc_id': sample_input_json["_id"],
                      'data': sample_input_json,
                      "source_name": sample_input_json["source_name"]
                      }
        print("Input:", json.dumps(input_data))
        doc = etk.create_document(input_data)
        docs = etk.process_ems(doc)

        for doc in docs[1:]:
            print(json.dumps(doc, indent=2))


