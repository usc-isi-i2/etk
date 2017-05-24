import dateparser


def parse_date(str_date):
    try:
        return dateparser.parse(str_date)
    except Exception as e:
        print 'Exception: {}, failed to parse {} as date'.format(e, str_date)
        return None


def convert_to_iso_format(date):
    try:
        return date.isoformat()
    except Exception as e:
        print 'Exception: {}, failed to convert {} to isoformat '.format(e, date)
        return None

