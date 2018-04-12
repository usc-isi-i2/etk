import unittest
from etk.timeseries_processor import TimeseriesProcessor
from etk.etk import ETK

etk = ETK()


class TestTimeseriesProcessor(unittest.TestCase):
    def test_excel_file(self) -> None:
        annotation = 'etk/timeseries/DIESEL_june_annotation.json'
        spreadsheet = 'etk/unit_tests/ground_truth/DIESEL_june_2017.xlsx'

        timeseriesProcessor = TimeseriesProcessor(etk=etk, annotation=annotation, spreadsheet=spreadsheet)
        # docs = timeseriesProcessor.timeseries_extractor()
        docs = [doc.cdr_document for doc in timeseriesProcessor.timeseries_extractor()]
        selected_docs = docs[1]
        selected_expected_docs = {
            'metadata': {'location': 'Abuja',
               'monthly_change': '6.3701923076923075',
               'name': 'AVERAGE DIESEL (AUTOMATIVE GAS OIL) PRICES/ Litre NGN',
               'provenance': {'filename': 'DIESEL_june_2017.xlsx',
                              'row': 5,
                              'sheet': 0},
               'yearly_change': '23.776223776223777'},
            'ts': [('2016-09-01', 189.5),
                ('2016-10-01', 180),
                ('2016-11-01', 182.5),
                ('2016-12-01', 180),
                ('2017-01-01', 270),
                ('2017-02-01', 252),
                ('2017-03-01', 255),
                ('2017-04-01', 248),
                ('2017-05-01', 208),
                ('2017-06-01', 221.25)]
        }

        self.assertEqual(selected_docs, selected_expected_docs)

