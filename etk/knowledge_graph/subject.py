from etk.knowledge_graph.node import URI, BNode, Literal
from etk.etk_exceptions import InvalidParameter


class Subject(object):
    def __init__(self, s):
        if not isinstance(s, URI) and not isinstance(s, BNode):
            raise InvalidParameter('Subject needs to be URI or BNode')
        self._resource = dict()
        self._s = s

    def add_property(self, p, o, reify=None):
        """

        :param p: URI, predicate
        :param o: [URI, BNode, Literal, Subject], object
        :param reify: [None, (URI, [None, URI, BNode])], reification
                    None -> don't reify
                    (URI, None) -> reify, and generate BNode statement
                    (URI, [URI, BNode]) -> reify, use provided statement term
        :return: None if not reify else statement term
        """
        if not isinstance(p, URI):
            raise InvalidParameter('Predict needs to be URI')
        if not self.__is_valid_object(o):
            raise InvalidParameter('Object needs to be URI or BNode or Literal or Triple')
        statement = None
        if reify:
            if isinstance(reify, URI):
                p_, s_ = (reify, None)
            elif isinstance(reify, tuple):
                p_, s_ = reify
            else:
                raise InvalidParameter('Reification paremeter is invalid')
            if not isinstance(p_, URI):
                raise InvalidParameter('Reification predicate needs to be URI')
            if s_ is None:
                s_ = BNode()
            if not isinstance(s_, URI) and not isinstance(s_, BNode):
                raise InvalidParameter('Reification statement needs to be URI or BNode')
            statement = Subject(s_)
            statement.add_property(p_, o)
            self.add_property(p_, statement)

        if p not in self._resource:
            self._resource[p] = set([])
        self._resource[p].add(o)

        return statement

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
                if not self._resource[p]:
                    del self._resource[p]
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
