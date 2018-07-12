from etk.etk_module import ETKModule
from etk.document import Document
from etk.extractors.date_extractor import DateExtractor
import datetime


class ETKModuleDate(ETKModule):
    def __init__(self, etk):
        ETKModule.__init__(self, etk)
        self.date_extractor = DateExtractor(self.etk, 'test_date_parser')

    def process_document(self, doc: Document):

        descriptions = doc.select_segments("date_description")
        date_text = doc.select_segments("date_description.text")

        ignore_before = datetime.datetime(1890, 1, 1)
        ignore_after = datetime.datetime(2500, 10, 10)
        relative_base = datetime.datetime(2018, 1, 1)

        for d, p in zip(date_text, descriptions):
            extracted_date = doc.extract(
                self.date_extractor,
                d,
                extract_first_date_only=False,   # first valid

                additional_formats=['%Y@%m@%d', '%a %Y, %b %d'],

                use_default_formats=True,

                # ignore_dates_before: datetime.datetime = None,
                ignore_dates_before=ignore_before,

                # ignore_dates_after: datetime.datetime = None,
                ignore_dates_after=ignore_after,

                detect_relative_dates=False,

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

                # prefer_dates_from: str = "current"  # can be "future", "future", "past".)
                prefer_dates_from='future',

                # date_value_resolution: DateResolution = DateResolution.DAY
            )

            p.store(extracted_date, "extracted_date")

        doc.kg.add_doc_value("date", "date_description.extracted_date[*]")