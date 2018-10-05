from etk.knowledge_graph.node import Node, URI, BNode, Literal


class Triples(object):
    def __init__(self, s):
        self._resource = dict()
        self._s = s

    def add_property(self, p, o):
        if p not in self._resource:
            self._resource[p] = set([])
        self._resource[p].add(o)

    def remove_property(self, p, o=None):
        try:
            if not o:
                del self._resource[p]
            else:
                self._resource[p].remove(o)
        except KeyError:
            return False

        return True

    def __iter__(self):
        for p in self._resource.keys():
            for o in self._resource[p]:
                yield self._s, p, o

    def __next__(self):
        return self.__iter__()
