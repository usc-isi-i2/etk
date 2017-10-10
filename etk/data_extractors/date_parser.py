import dateparser
import datetime


def parse_date(str_date, ignore_future_dates=True, ignore_past_years=20, strict_parsing=True):
    try:
        if strict_parsing:
            parsed_date = dateparser.parse(str_date, settings={'STRICT_PARSING': True})
        else:
            parsed_date = dateparser.parse(str_date)
        if parsed_date:
            parsed_year = parsed_date.year
            current_year = datetime.datetime.now().year
            if current_year - ignore_past_years > parsed_year:
                return None
            if ignore_future_dates:
                return parsed_date if datetime.datetime.now() >= parsed_date else None
        return parsed_date
    except Exception as e:
        print 'Exception: {}, failed to parse {} as date'.format(e, str_date)
        return None


def convert_to_iso_format(date):
    try:
        return date.isoformat() if date else None
    except Exception as e:
        print 'Exception: {}, failed to convert {} to isoformat '.format(e, date)
        return None
