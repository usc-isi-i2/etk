from enum import Enum, auto


class ErrorPolicy(Enum):
    """
    THROW_EXTRACTION: throw error extraction and return
    THROW_DOCUMENT: throw whole document
    PROCESS: still process by converting to processable type
    RAISE: raise error
    """
    THROW_EXTRACTION = auto()
    THROW_DOCUMENT = auto()
    PROCESS = auto()
    RAISE = auto()


class ETKValueError(Exception):
    pass


class KgValueError(ETKValueError):
    def __init__(self, message=""):
        super(KgValueError, self).__init__(message)


class ExtractorValueError(ETKValueError):
    def __init__(self, message=""):
        super(ExtractorValueError, self).__init__(message)


class TokenizerValueError(ETKValueError):
    def __init__(self, message=""):
        super(TokenizerValueError, self).__init__(message)


class ISODateError(Exception):
    def __init__(self, message=""):
        super(ISODateError, self).__init__(message)


class InvalidJsonPathError(Exception):
    def __init__(self, message=""):
        super(InvalidJsonPathError, self).__init__(message)


class StoreExtractionError(Exception):
    def __init__(self, message=""):
        super(StoreExtractionError, self).__init__(message)


class NotGetETKModuleError(Exception):
    def __init__(self, message=""):
        super(NotGetETKModuleError, self).__init__(message)


class InvalidArgumentsError(Exception):
    def __init__(self, message=""):
        super(InvalidArgumentsError, self).__init__(message)


class InvalidFilePathError(Exception):
    def __init__(self, message=""):
        super(InvalidFilePathError, self).__init__(message)


class ExtractorError(Exception):
    def __init__(self, message=""):
        super(ExtractorError, self).__init__(message)


class UndefinedFieldError(Exception):
    def __init__(self, message=""):
        super(UndefinedFieldError, self).__init__(message)
