from etk.knowledge_graph.triples import Triples


class Graph(object):
    def __init__(self):
        self._g = object()
        self._ns = object()

    def add_triples(self, triples: Triples):
        # TODO: expend namespace

        self._g.add(triples)

    def add_triple(self, s, p, o):
        t = Triples(s)
        t.add_property(p, o)
        self.add_triples(t)

    def parse(self, content, format='turtle'):
        pass

    def serialize(self, format='legacy', namespace_manager=None):
        pass
