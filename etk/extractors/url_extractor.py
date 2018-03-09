from etk.extractors.regex_extractor import RegexExtractor


class URLExtractor(RegexExtractor):
    def __init__(self, allow_missing_http: bool=False):
        if allow_missing_http:
            # reference: https://gist.github.com/dperini/729294, slightly modified to match _ and allow missing "http"
            url_pattern = u"(?:(?:https?|ftp)://)?"\
                    u"(?:\S+(?::\S*)?@)?(?:"\
                    u"(?!(?:10|127)(?:\.\d{1,3}){3})"\
                    u"(?!(?:169\.254|192\.168)(?:\.\d{1,3}){2})"\
                    u"(?!172\.(?:1[6-9]|2\d|3[0-1])(?:\.\d{1,3}){2})"\
                    u"(?:[1-9]\d?|1\d\d|2[01]\d|22[0-3])"\
                    u"(?:\.(?:1?\d{1,2}|2[0-4]\d|25[0-5])){2}"\
                    u"(?:\.(?:[1-9]\d?|1\d\d|2[0-4]\d|25[0-4]))|"\
                    u"(?:(?:[a-z\u00a1-\uffff0-9][_-]?)*[a-z\u00a1-\uffff0-9]+)"\
                    u"(?:\.(?:[a-z\u00a1-\uffff0-9][_-]?)*[a-z\u00a1-\uffff0-9]+)*"\
                    u"(?:\.(?:[a-z\u00a1-\uffff]{2,})))(?::\d{2,5})?(?:/\S*)?"
        else:
            # reference: https://gist.github.com/dperini/729294, slightly modified to match _
            url_pattern = u"(?:(?:https?|ftp)://)"\
                    "(?:\S+(?::\S*)?@)?(?:"\
                    u"(?!(?:10|127)(?:\.\d{1,3}){3})"\
                    u"(?!(?:169\.254|192\.168)(?:\.\d{1,3}){2})"\
                    u"(?!172\.(?:1[6-9]|2\d|3[0-1])(?:\.\d{1,3}){2})"\
                    u"(?:[1-9]\d?|1\d\d|2[01]\d|22[0-3])"\
                    u"(?:\.(?:1?\d{1,2}|2[0-4]\d|25[0-5])){2}"\
                    u"(?:\.(?:[1-9]\d?|1\d\d|2[0-4]\d|25[0-4]))|"\
                    u"(?:(?:[a-z\u00a1-\uffff0-9][_-]?)*[a-z\u00a1-\uffff0-9]+)"\
                    u"(?:\.(?:[a-z\u00a1-\uffff0-9][_-]?)*[a-z\u00a1-\uffff0-9]+)*"\
                    u"(?:\.(?:[a-z\u00a1-\uffff]{2,})))(?::\d{2,5})?(?:/\S*)?"
        RegexExtractor.__init__(self, pattern=url_pattern, extractor_name="url extractor")
