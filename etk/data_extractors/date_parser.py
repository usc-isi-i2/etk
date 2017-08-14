import dateparser
import datetime


def parse_date(str_date, ignore_future_dates=True):
    try:
        parsed_date = dateparser.parse(str_date)
        if ignore_future_dates:
            return parsed_date if datetime.datetime.now() >= parsed_date else None
        return parsed_date
    except Exception as e:
        print 'Exception: {}, failed to parse {} as date'.format(e, str_date)
        return None


def convert_to_iso_format(date):
    try:
        return date.isoformat()
    except Exception as e:
        print 'Exception: {}, failed to convert {} to isoformat '.format(e, date)
        return None
