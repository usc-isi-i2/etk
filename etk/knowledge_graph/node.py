from uuid import uuid4
from time import time
from datetime import date, datetime
from xml.dom.minidom import Document, DocumentFragment
from etk.etk_exceptions import InvalidGraphNodeValueError, UnknownLiteralType


class Node(object):
    def __init__(self, value):
        if not isinstance(value, str):
            raise InvalidGraphNodeValueError()
        self._value = value

    def __eq__(self, other):
        if not isinstance(other, Node):
            return False
        return self.value == other.value

    def __hash__(self):
        return hash(self.value)

    @property
    def value(self):
        return self._value

    def is_valid(self):
        raise NotImplementedError('Subclass should implement this.')


class URI(Node):
    def __init__(self, value):
        if isinstance(value, URI):
            super().__init__(value.value)
        else:
            super().__init__(value)

    def __eq__(self, other):
        if not isinstance(other, URI):
            return False
        return super().__eq__(other)

    def __hash__(self):
        return hash(self.value)

    def is_valid(self):
        return self.value is not None


class BNode(Node):
    def __init__(self, value=None):
        if isinstance(value, BNode):
            super().__init__(value.value)
        else:
            if not value:
                value = uuid4().hex
            super().__init__(value)

    def __eq__(self, other):
        if not isinstance(other, BNode):
            return False
        return super().__eq__(other)

    def __hash__(self):
        return hash(self.value)

    def is_valid(self):
        return self.value is not None


class Literal(Node):
    def __init__(self, value, lang=None, type_=None):
        if isinstance(value, Literal):
            super().__init__(value.value)
            self._lang = value.lang
            self._type = value._type
        else:
            super().__init__(value)
            self._lang = lang
            if type_ and isinstance(type_, str):
                type_ = LiteralType(type_)
            self._type = type_

    def __eq__(self, other):
        if not isinstance(other, Literal):
            return False
        return super().__eq__(other) and self.lang == other.lang and self.type == other.type

    def __hash__(self):
        return hash((self.value, self.lang, self.raw_type))

    def __str__(self):
        return self.value

    @property
    def lang(self):
        return self._lang

    @property
    def type(self):
        return self._type

    @property
    def raw_type(self):
        if self._type:
            return self._type.value

    def is_valid(self):
        return self.value is not None and not (self._lang and self._type != LiteralType.string)


class __Type(type):
    def __getattr__(self, item):
        return LiteralType(item)


class LiteralType(URI, metaclass=__Type):
    def __init__(self, s):
        super().__init__(self._resolve(s))

    def _resolve(self, s):
        if not isinstance(s, str):
            raise UnknownLiteralType()
        if s.startswith(self.xsd) and s[len(self.xsd):] in self.xsd_tokens or \
                s.startswith(self.rdf) and s[len(self.rdf)] in self.rdf_tokens:
            return s
        elif s.startswith('xsd:'):
            s = s[4:]
            if s in self.xsd_tokens:
                return self.xsd + s
        elif s.startswith('rdf:'):
            s = s[4:]
            if s in self.rdf_tokens:
                return self.rdf + s
        elif self._is_valid_field(s):
            if s in self.xsd_tokens:
                return self.xsd + s
            elif s in self.rdf_tokens:
                return self.rdf + s
        raise UnknownLiteralType()

    def _is_valid_field(self, s):
        return s in self.xsd_tokens or s in self.rdf_tokens

    xsd = 'http://www.w3.org/2001/XMLSchema#'
    rdf = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#'
    xsd_tokens = {
        'time', 'date', 'dateTime', 'string', 'normalizedString', 'token', 'language', 'boolean', 'decimal', 'integer',
        'nonPositiveInteger', 'long', 'nonNegativeInteger', 'negativeInteger', 'int', 'unsignedLong', 'positiveInteger',
        'short', 'unsignedInt', 'byte', 'unsignedShort', 'unsignedByte', 'float', 'double', 'base64Binary', 'anyURI'
    }
    rdf_tokens = {'XMLLiteral', 'HTML'}
    to_python_type = {
        'time': time,
        'date': date,
        'dateTime': datetime,
        'string': str,
        'normalizedString': str,
        'token': str,
        'language': str,
        'boolean': bool,
        'decimal': float,
        'integer': int,
        'nonPositiveInteger': int,
        'long': int,
        'nonNegativeInteger': int,
        'negativeInteger': int,
        'int': int,
        'unsignedLong': int,
        'positiveInteger': int,
        'short': int,
        'unsignedInt': int,
        'byte': int,
        'unsignedShort': int,
        'unsignedByte': int,
        'float': float,
        'double': float,
        'base64Binary': str,
        'anyURI': str,
        'XMLLiteral': Document,
        'HTML': DocumentFragment
    }
