from etk.knowledge_graph.node import URI, BNode, Literal


class InvalidParameter(Exception):
    pass


class Subject(object):
    def __init__(self, s):
        if not isinstance(s, URI) and not isinstance(s, BNode):
            raise InvalidParameter('Subject needs to be URI or BNode')
        self._resource = dict()
        self._s = s

    def add_property(self, p, o):
        if not isinstance(p, URI):
            raise InvalidParameter('Predict needs to be URI')
        if not self.__is_valid_object(o):
            raise InvalidParameter('Object needs to be URI or BNode or Literal or Triple')

        if p not in self._resource:
            self._resource[p] = set([])
        self._resource[p].add(o)

    def remove_property(self, p, o=None):
        if not isinstance(p, URI):
            raise InvalidParameter('Predicate needs to be URI')
        if o and not self.__is_valid_object(o):
            raise InvalidParameter('Object needs to be URI or BNode or Literal or Triple')

        try:
            if not o:
                del self._resource[p]
            else:
                self._resource[p].remove(o)
        except KeyError:
            return False

        return True

    @property
    def subject(self):
        return self._s

    @staticmethod
    def __is_valid_object(o):
        if isinstance(o, URI) or isinstance(o, BNode) or isinstance(o, Literal) or isinstance(o, Subject):
            return True
        return False

    def __iter__(self):
        for p, os in self._resource.items():
            for o in os:
                yield self._s, p, o

    def __next__(self):
        return self.__iter__()
