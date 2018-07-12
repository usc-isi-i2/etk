import json, os, sys, datetime
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from etk.etk import ETK
from etk.extractors.date_extractor import DateExtractor, DateResolution
from etk.etk_module import ETKModule


class DateETKModule(ETKModule):
    """
    Abstract class for extraction module
    """
    def __init__(self, etk):
        ETKModule.__init__(self, etk)
        self.date_extractor = DateExtractor(self.etk, 'test_date_parser')

    def process_document(self, doc):
        """
        Add your code for processing the document
        """
        input_text = doc.select_segments("input")[0]
        format = doc.select_segments('format')[0]

        ignore_before = datetime.datetime(1890, 1, 1)
        ignore_after = datetime.datetime(2500, 10, 10)
        relative_base = datetime.datetime(2018, 1, 1)

        dates = doc.extract(
            self.date_extractor,
            input_text,
            extract_first_date_only=False,  # first valid

            additional_formats=[format._value],

            use_default_formats=False,

            # ignore_dates_before: datetime.datetime = None,
            ignore_dates_before=ignore_before,

            # ignore_dates_after: datetime.datetime = None,
            ignore_dates_after=ignore_after,

            detect_relative_dates=not format._value,

            relative_base=relative_base,

            # preferred_date_order: str = "MDY",  # used for interpreting ambiguous dates that are missing parts
            preferred_date_order="DMY",

            prefer_language_date_order=True,

            # timezone: str = None,  # default is local timezone.
            # timezone='GMT',

            # to_timezone: str = None,  # when not specified, not timezone conversion is done.
            # to_timezone='UTC',

            # return_as_timezone_aware: bool = True
            return_as_timezone_aware=False,

            # prefer_day_of_month: str = "first",  # can be "current", "first", "last".
            prefer_day_of_month='first',

            # prefer_dates_from: str = "current"  # can be "current", "future", "past".)
            prefer_dates_from='current',

            # date_value_resolution: DateResolution = DateResolution.DAY
            date_value_resolution = DateResolution.SECOND if format._value and format._value[1] in ['H','I'] else DateResolution.DAY
        )
        doc.select_segments('$')[0].store(dates, "extracted_date")


if __name__ == "__main__":

    with open('date_ground_truth.txt', 'r') as f:
        texts = f.readlines()

    etk = ETK(modules=DateETKModule)
    res = []
    for text in texts:
        text = text.strip()
        if text and text[0] != '#':
            temp = text.split('|')
            if len(temp) == 3:
                input_text, expected, format = temp
                doc = etk.create_document({'input': input_text, 'expected': expected, 'format': format})
                docs= etk.process_ems(doc)
                res.append(docs[0].value)

    for r in res:
        extracted = r['extracted_date'][0] if 'extracted_date' in r and r['extracted_date'] else '            '
        expected = r['expected'].replace('@today', datetime.datetime.now().isoformat()[:10])
        print('extracted: ', extracted,
              '\texpected:', expected,
              '\tSAME' if extracted == expected else '\t    ',
              '\tinput: ', r['input'],
              '\tformat: ', r['format'], )