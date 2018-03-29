class KgValueInvalidError(Exception):
    def __init__(self, message=""):
        super(KgValueInvalidError, self).__init__(message)


class ISODateError(Exception):
    def __init__(self, message=""):
        super(ISODateError, self).__init__(message)


class InvalidJsonPathError(Exception):
    def __init__(self, message=""):
        super(InvalidJsonPathError, self).__init__(message)
