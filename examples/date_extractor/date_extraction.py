import json, os, sys, datetime
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from etk.etk import ETK
from etk.extractors.date_extractor import DateExtractor


with open('date_ground_truth.txt', 'r') as f:
    text = f.read()

sample_input = {
    "projects": [
        {
            "name": "etk",
            "description": "version 2 of etk, implemented by Runqi, Dongyu, Sylvia, Amandeep and others."
        },
        {
            "name": "rltk",
            "description": "record linkage toolkit, implemented by Pedro, Mayank, Yixiang and several students."
        }
    ],
    "members": [
        {
            "name": "Dongyu Li",
            "description": "1989-12-12 03/05/2018: I went to USC on Aug 20th, 2016 and will graduate on 2018, May 11. "
                           "My birthday is 29-04-1994. I will graduate in 5 days. My classmates traveled to Hawaii last month. "
                           "I hope that I could have a vocation after two weeks. Last year I was very busy. yesterday, The day after Tomorrow"
            # "description": text
        }
    ]
}


etk = ETK()
doc = etk.create_document(sample_input)

descriptions = doc.select_segments("projects[*].description")
projects = doc.select_segments("projects[*]")

date_extractor = DateExtractor(etk, 'test_date_parser')
member_descriptions = doc.select_segments("members[*].description")
members = doc.select_segments("members[*]")

ignore_before = datetime.datetime(1990, 1, 1)
ignore_after = datetime.datetime(2020, 10, 10)
relative_base = datetime.datetime(2018, 1, 1)

for m_d, m in zip(member_descriptions, members):
    dates = doc.invoke_extractor(
        date_extractor,
        m_d,
        extract_first_date_only=False,   # first valid

        additional_formats=['%d-%m-%Y', '%b %dth, %Y'],

        # match relax or strict / can be partial match ?     # TODO

        use_default_formats=True,

        # ignore_dates_before: datetime.datetime = None,
        ignore_dates_before=ignore_before,

        # ignore_dates_after: datetime.datetime = None,
        ignore_dates_after=ignore_after,

        detect_relative_dates=True,    # TODO

        relative_base=relative_base,    # TODO

        # preferred_date_order: str = "MDY",  # used for interpreting ambiguous dates that are missing parts
        preferred_date_order="DMY",

        prefer_language_date_order=True,    # TODO

        # timezone: str = None,  # default is local timezone.
        timezone='GMT',

        # to_timezone: str = None,  # when not specified, not timezone conversion is done.
        to_timezone='UTC',

        # return_as_timezone_aware: bool = True,  # when false make dates timezone not aware
        return_as_timezone_aware=True,     # TODO

        # prefer_day_of_month: str = "first",  # can be "current", "first", "last".
        prefer_day_of_month='last',

        # prefer_dates_from: str = "current"  # can be "future", "future", "past".)
        prefer_dates_from='future',    # TODO
    )
    m.store_extractions(dates, "related_dates")


print(json.dumps(sample_input, indent=2))
