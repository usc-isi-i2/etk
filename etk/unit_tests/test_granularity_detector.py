import unittest
from etk.timeseries.annotation.granularity_detector import GranularityDetector
from datetime import datetime, date


class TestGranularityDetector(unittest.TestCase):
    def test_granularity_detection(self):
        dates1 = ["2001-01-01", "2001-01-02", "2001-01-03", "2001-01-04"]
        dates2 = ["2001-01-01"]
        dates3 = ["2001", "2002", "2003", "2004", "2005"]
        dates4 = ["2001-01-01", "2001-04-01", "2001-07-01", "2001-10-01"]
        dates5 = ["2010-01-01", "2010-01-02", "2010-01-03", "cant parse this", "2010-01-05", "2010-01-06"]
        # dates6 = ["2001Q1", "2001Q2", "2001Q3"]

        assert GranularityDetector.get_granularity(dates1, return_best=True) == "day"
        assert GranularityDetector.get_granularity(dates2, return_best=True) == "unknown"
        assert GranularityDetector.get_granularity(dates3, return_best=True) == "year"
        assert GranularityDetector.get_granularity(dates4, return_best=True) == "quarter"
        assert GranularityDetector.get_granularity(dates5, return_best=True) == "day"
        # assert GranularityDetector.get_granularity(dates6, return_best=True) == "quarterly"

    def test_date_parser(self):
        date1 = "\ufeff2001-01-01"
        date2 = datetime(2001, 1, 1)
        date3 = None
        date4 = "garbage"
        date5 = 1
        date6 = date(2001, 1, 1)
        date7 = "2018-10-08 20:20:55.130874+00:00"

        assert GranularityDetector.get_parsed_date(date1) == datetime(2001, 1, 1)
        assert GranularityDetector.get_parsed_date(date2) == datetime(2001, 1, 1)
        assert GranularityDetector.get_parsed_date(date3) == None
        assert GranularityDetector.get_parsed_date(date4) == None
        assert GranularityDetector.get_parsed_date(date5) == None
        assert GranularityDetector.get_parsed_date(date6) == datetime(2001, 1, 1)
        assert GranularityDetector.get_parsed_date(date7) == datetime(2018, 10, 8, 20, 20,
                                                                      00)  # Seconds resolution is lost
