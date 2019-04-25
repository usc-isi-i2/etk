from etk.knowledge_graph.node import URI, Literal, LiteralType
from etk.knowledge_graph.subject import Subject
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
    full_value = None
    normalized_value = None

    def _v_name(self):
        raise NotImplemented

    def _create_full_value(self):
        self.full_value = Subject(URI('wdv:' + self._v_name()))


class Item(Value):
    type = URI('wikibase:WikibaseItem')
    def __init__(self, s):
        super().__init__()
        self.value = URI('wd:'+s)


class Property(Value):
    type = URI('wikibase:WikibaseProperty')
    def __init__(self, s):
        super().__init__()
        self.value = URI('wd:'+s)


class TimeValue(Value):
    type = URI('wikibase:Time')
    def __init__(self, value, calendar, precision, time_zone):
        super().__init__()
        self.value = Literal(value, type_=LiteralType.date)
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

        self.__build_full_value()

    def __build_full_value(self):
        self._create_full_value()
        self.full_value.add_property(URI('rdf:type'), URI('wikibase:Time'))
        self.full_value.add_property(URI('wikibase:timePrecision'), self._precision)
        self.full_value.add_property(URI('wikibase:timeTimezone'), self._time_zone)
        self.full_value.add_property(URI('wikibase:timeCalendarModel'), self._calendar.value)

    def _v_name(self):
        return str(hash(self.value.value + self._calendar.value.value + self._precision.value + self._time_zone.value))


class ExternalIdentifier(Value):
    type = URI('wikibase:ExternalId')
    def __init__(self, s, normalized_value=None):
        super().__init__()
        self.value = Literal(s, type_=LiteralType.string)
        self.nomalized_value = normalized_value is not None and URI(normalized_value)


class QuantityValue(Value):
    type = URI('wikibase:Quantity')
    def __init__(self, amount, unit=None, upper_bound=None, lower_bound=None, normalized=True):
        self.value = Literal(str(amount), type_=LiteralType.decimal)
        self.upper_bound = upper_bound is not None and Literal(upper_bound, type_=LiteralType.decimal)
        self.lower_bound = lower_bound is not None and Literal(lower_bound, type_=LiteralType.decimal)
        self.unit = unit is not None and unit
        self.__build_full_value()
        if isinstance(normalized, QuantityValue):
            self.normalized_value = normalized.full_value
        else:
            self.normalized_value = self.full_value
        self.full_value.add_property(URI('wikibase:quantityNormalized'), self.normalized_value)


    def __build_full_value(self):
        self._create_full_value()
        self.full_value.add_property(URI('rdf:type'), URI('wikibase:QuantityValue'))
        self.full_value.add_property(URI('wikibase:quantityAmount'), self.value)
        if self.upper_bound:
            self.full_value.add_property(URI('wikibase:quantityUpperBound'), self.upper_bound)
        if self.lower_bound:
            self.full_value.add_property(URI('wikibase:quantityLowerBound'), self.lower_bound)
        if self.unit:
            self.full_value.add_property(URI('wikibase:quantityUnit'), self.unit.value)


    def _v_name(self):
        # TODO: modify this v_name generate method
        return '123'


class StringValue(Value):
    type = URI('wikibase:String')
    def __init__(self, s):
        super().__init__()
        self.value = Literal(s, type_=LiteralType.string)


class URLValue(Value, URI):
    type = URI('wikibase:Url')
    def __init__(self, s):
        super().__init__(s)
        self.value = URI(s)


class GlobalCoordinate(Value):
    type = URI('wikibase:GlobeCoordinate')
    def __init__(self, latitude, longitude, precision, globe=None):
        self.globe = globe
        self.latitude = Literal(latitude, type_=LiteralType.decimal)
        self.longitude = Literal(longitude, type_=LiteralType.decimal)
        self.precision = Literal(precision, type_=LiteralType.decimal)
        s = 'Point({} {})'.format(latitude, longitude)
        if globe:
            s = '<{}> {}'.format(globe.value.value.replace('wd:', 'http://www.wikidata.org/entity/'), s)
        self.value = Literal(s, type_='geo:wktLiteral')
        self.__build_full_value()

    def __build_full_value(self):
        self._create_full_value()
        self.full_value.add_property(URI('rdf:type'), URI('wikibase:GlobecoordinateValue'))
        self.full_value.add_property(URI('wikibase:geoGlobe'), )
        self.full_value.add_property(URI('wikibase:geoLatitude'), )
        self.full_value.add_property(URI('wikibase:geoLongitude'), )
        self.full_value.add_property(URI('wikibase:geoPrecision'), )

    def _v_name(self):
        return '123'


class MonolingualText(Value):
    type = URI('wikibase:Monolingualtext')
    def __init__(self, s, lang):
        self.value = Literal(s, lang=lang)
