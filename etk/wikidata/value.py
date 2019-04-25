from etk.knowledge_graph.node import URI, Literal, LiteralType
from enum import Enum


class Precision(Enum):
    # https://www.wikidata.org/wiki/Help:Dates#Precision
    day = Literal('11', type_=LiteralType.integer)
    month = Literal('10', type_=LiteralType.integer)
    year = Literal('9', type_=LiteralType.integer)
    decade = Literal('8', type_=LiteralType.integer)
    century = Literal('7', type_=LiteralType.integer)
    millennium = Literal('6', type_=LiteralType.integer)
    hundred_thousand_years = Literal('4', type_=LiteralType.integer)
    million_years = Literal('3', type_=LiteralType.integer)
    billion_years = Literal('0', type_=LiteralType.integer)


class Value:
    value = None
    predicate = None
    type = None

    @property
    def properties(self):
        raise NotImplementedError


class TimeValue(Value):
    # Point in time
    def __init__(self, value, calendar, precision, time_zone):
        super().__init__()
        self.value = Literal(value, type_=LiteralType.date)
        self.predicate = URI('wikibase:timeValue')
        self.type = URI('wikibase:TimeValue')

        self._calendar = calendar

        if isinstance(precision, Precision):
            self._precision = precision.value
        elif isinstance(precision, Literal):
            self._precision = precision
        else:
            self._precision = Literal(str(precision), type_=LiteralType.integer)

        if isinstance(time_zone, Literal):
            self._time_zone = time_zone
        else:
            self._time_zone = Literal(str(time_zone), type_=LiteralType.integer)

    @property
    def properties(self):
        if self._calendar:
            yield URI('wikibase:timeCalendarModel'), self._calendar
        if self._precision:
            yield URI('wikibase:timePrecision'), self._precision
        if self._time_zone:
            yield URI('wikibase:timeTimezone'), self._time_zone


class ExternalIdentifier(Value):
    def __init__(self, s, normalized_value=None):
        super().__init__()
        self.value = URI(s)
        self.nomalized_value = normalized_value


class QuantityValue(Value):
    def __init__(self, ):
        pass


class StringValue(Value):
    raise NotImplementedError


class GlobalCoordinate(Value):
    raise NotImplementedError


class MonolingualText(Value):
    raise NotImplementedError


class PropertyValue(Value):
    raise NotImplementedError

class URLValue(Value):
    raise NotImplementedError
