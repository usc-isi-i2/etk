import unittest, json
from etk.timeseries_processor import TimeseriesProcessor
from etk.etk import ETK
from etk.knowledge_graph import KGSchema

kg_schema = KGSchema(json.load(open('etk/unit_tests/ground_truth/test_config.json')))

etk = ETK(kg_schema=kg_schema)


# python -m unittest etk.unit_tests.test_timeseries_processor to run all unittests

class TestTimeseriesProcessor(unittest.TestCase):
    def test_excel_file(self) -> None:
        annotation = 'etk/timeseries/DIESEL_june_annotation.json'
        spreadsheet = 'etk/unit_tests/ground_truth/DIESEL_june_2017.xlsx'

        timeseriesProcessor = TimeseriesProcessor(etk=etk, annotation=annotation, spreadsheet=spreadsheet)
        docs = [doc.cdr_document for doc in timeseriesProcessor.timeseries_extractor()]
        selected_docs = docs[1]
        expected_metadata = {
            "name": "AVERAGE DIESEL (AUTOMATIVE GAS OIL) PRICES/ Litre NGN",
            "granularity": "monthly",
            "provenance": {
                "filename": "DIESEL_june_2017.xlsx",
                "sheet": 0,
                "row": 5
            },
            "location": "Abuja",
            "yearly_change": "23.776223776223777",
            "monthly_change": "6.3701923076923075",
            "uid": "5e8c87d3d291944f01f6084c5dee223ee31746e2"
        }
        expected_ts = [('2016-09-01', 189.5),
                       ('2016-10-01', 180),
                       ('2016-11-01', 182.5),
                       ('2016-12-01', 180),
                       ('2017-01-01', 270),
                       ('2017-02-01', 252),
                       ('2017-03-01', 255),
                       ('2017-04-01', 248),
                       ('2017-05-01', 208),
                       ('2017-06-01', 221.25)]
        actual_ts = []
        for t in selected_docs['ts']:
            if 'instant' in t['time'].keys():
                actual_ts.append((t['time']['instant'], t['value']))
            else:
                actual_ts.append((t['time']['span']['start_time'], t['value']))

        self.assertEqual(actual_ts, expected_ts)
        for k in expected_metadata.keys():
            self.assertEqual(selected_docs['metadata'][k], expected_metadata[k])

    def testSimpleExtraction(self):

        annotation = 'etk/unit_tests/ground_truth/ts_csv.csv_annotation.json'
        spreadsheet = 'etk/unit_tests/ground_truth/ts_csv.csv'

        timeseriesProcessor = TimeseriesProcessor(etk=etk, annotation=annotation, spreadsheet=spreadsheet)
        docs = [doc.cdr_document for doc in timeseriesProcessor.timeseries_extractor()]
        selected_docs = docs[0]

        actual_ts = []
        for t in selected_docs['ts']:
            if 'instant' in t['time'].keys():
                actual_ts.append((t['time']['instant'], t['value']))
            else:
                actual_ts.append((t['time']['span']['start_time'], t['value']))

        assert actual_ts == [(u'1/1/18', 1234.123), (u'2/1/18', 1241.243), (u'3/1/18', 1254.23), (u'4/1/18', 1674.46),
                             (u'5/1/18', 1252.12), (u'6/1/18', 1178.323), (u'7/1/18', 1265.34), (u'8/1/18', 1154.345),
                             (u'9/1/18', 1253.345), (u'10/1/18', 1325.52), (u'11/1/18', 1345.346),
                             (u'12/1/18', 1234.345)]

    def testExtractionSucceedsWhenMetadataPropertyIsEmpty(self):
        annotation = 'etk/unit_tests/ground_truth/ts_csv.csv_annotation2.json'
        spreadsheet = 'etk/unit_tests/ground_truth/ts_csv.csv'

        timeseriesProcessor = TimeseriesProcessor(etk=etk, annotation=annotation, spreadsheet=spreadsheet)
        docs = [doc.cdr_document for doc in timeseriesProcessor.timeseries_extractor()]
        selected_docs = docs[0]

        actual_ts = []
        for t in selected_docs['ts']:
            if 'instant' in t['time'].keys():
                actual_ts.append((t['time']['instant'], t['value']))
            else:
                actual_ts.append((t['time']['span']['start_time'], t['value']))

        assert actual_ts == [(u'1/1/18', 1234.123), (u'2/1/18', 1241.243), (u'3/1/18', 1254.23), (u'4/1/18', 1674.46),
                             (u'5/1/18', 1252.12), (u'6/1/18', 1178.323), (u'7/1/18', 1265.34), (u'8/1/18', 1154.345),
                             (u'9/1/18', 1253.345), (u'10/1/18', 1325.52), (u'11/1/18', 1345.346),
                             (u'12/1/18', 1234.345)]
