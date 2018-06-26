import unittest, json
from etk.timeseries_processor import TimeseriesProcessor
from etk.etk import ETK
from etk.knowledge_graph_schema import KGSchema

kg_schema = KGSchema(json.load(open('etk/unit_tests/ground_truth/test_config.json')))

etk = ETK(kg_schema=kg_schema)


class TestTimeseriesProcessor(unittest.TestCase):
    def test_excel_file(self) -> None:
        annotation = 'etk/timeseries/DIESEL_june_annotation.json'
        spreadsheet = 'etk/unit_tests/ground_truth/DIESEL_june_2017.xlsx'

        timeseriesProcessor = TimeseriesProcessor(etk=etk, annotation=annotation, spreadsheet=spreadsheet)
        docs = [doc.cdr_document for doc in timeseriesProcessor.timeseries_extractor()]
        selected_docs = docs[1]
        expected_metadata = {
            "name": "AVERAGE DIESEL (AUTOMATIVE GAS OIL) PRICES/ Litre NGN",
            "provenance": {
                "filename": "DIESEL_june_2017.xlsx",
                "sheet": 0,
                "row": 5
            },
            "location": "Abuja",
            "yearly_change": "23.776223776223777",
            "monthly_change": "6.3701923076923075",
            "uid": "4ecb3b5cc3f65edf53550ddc2966684203706d9d"
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
