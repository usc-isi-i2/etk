from enum import Enum, auto


class FieldType(Enum):
    """
    TEXT: value must be a string
    NUMBER: value must be a number
    LOCATION: value must be a location format
    DATE: value must be a date format
    """
    STRING = auto()
    NUMBER = auto()
    LOCATION = auto()
    DATE = auto()
    KG_ID = auto()
