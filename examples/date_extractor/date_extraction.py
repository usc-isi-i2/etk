import json, os, sys, datetime, time
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from etk.etk import ETK
from etk.extractors.date_extractor import DateExtractor


with open('date_ground_truth.txt', 'r') as f:
    text = f.read()

sample_input = {
    "members": [
        {
            "name": "John",
            "description": text
        },
        {
            "name": "Ryan",
            "description": "I will graduate in 5 days. My classmates traveled to Hawaii last month. "
                           "I hope that I could have a vocation after two weeks. Last year I was very busy. "
                           "yesterday, The day after Tomorrow"
        },
        {
            "name": "Selina",
            "description": "May June JULY march dec"
        },
        {
            "name": "Sara",
            "description": "el 29 de febrero de 1996 vs lunes, el 24 de junio, 2013 vs 3 de octubre de 2017"
        },
        {
            "name": "Debbie",
            "description": "2010@3@29  Jul.$15$17     Thur 2018, Apr. 5"
        }
    ]
}
start = time.time()

etk = ETK()
doc = etk.create_document(sample_input)

date_extractor = DateExtractor(etk, 'test_date_parser')
member_descriptions = doc.select_segments("members[*].description")
members = doc.select_segments("members[*]")

ignore_before = datetime.datetime(1890, 1, 1)
ignore_after = datetime.datetime(2500, 10, 10)
relative_base = datetime.datetime(2018, 1, 1)

for m_d, m in zip(member_descriptions, members):
    dates = doc.invoke_extractor(
        date_extractor,
        m_d,
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
    )
    m.store_extractions(dates, "related_dates")
    print(len(dates))

print(json.dumps(sample_input, indent=2))
print(time.time()-start)