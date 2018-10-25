from etk.knowledge_graph.triples import Triples
from etk.knowledge_graph.node import URI, BNode, Literal
from etk.knowledge_graph.namespacemanager import NamespaceManager
from functools import lru_cache
import rdflib


class Graph(object):
    def __init__(self):
        self._g = rdflib.Graph()
        self._ns = NamespaceManager(self._g)

    def bind(self, prefix, namespace, override=True, replace=False):
        self._ns.bind(prefix, namespace, override, replace)

    def add_triples(self, triples, context=None):
        if not context:
            context = set()

        for t in triples:
            s, p, o = t
            if isinstance(o, Triples) and o not in context:
                context.add(o)
                self.add_triples(o, context)
                o = o.subject

            # convert triple to RDFLib recognizable format
            triple = self._convert_triple_rdflib((s, p, o))
            self._g.add(triple)

    def add_triple(self, s, p, o):
        t = Triples(s)
        t.add_property(p, o)
        self.add_triples(t)

    def parse(self, content, format='turtle'):
        self._g.parse(data=content, format=format)

    def serialize(self, format='ttl', namespace_manager=None):
        # may need some way to serialize ttl, json-ld
        if format.lower() in ('ttl', 'turtle'):
            b_string = self._g.serialize(format=format, namespace_manager=namespace_manager)
        elif format.lower() == 'json-ld':
            b_string = self._g.serialize(format=format, contexts=namespace_manager)
        else:
            b_string = self._g.serialize(format=format)
        return b_string.decode('UTF-8')

    @lru_cache()
    def _resolve_URI(self, uri: URI) -> rdflib.URIRef:
        """
        Convert a URI object into a RDFLib URIRef, including resolve its context

        :param uri: URI
        :return: rdflib.URIRef
        """
        return self._ns.parse_uri(uri.value)

    def _is_rdf_type(self, uri: URI) -> bool:
        if not isinstance(uri, URI):
            return False
        return self._resolve_URI(uri) == rdflib.RDF.type

    def _convert_triple_rdflib(self, triple):
        """
        Convert a Node triple into RDFLib triple
        """
        s, p, o = triple
        sub = self._resolve_URI(s) if isinstance(s, URI) else rdflib.BNode(s.value)
        pred = self._resolve_URI(p)
        if isinstance(o, URI):
            obj = self._resolve_URI(o)
        elif isinstance(o, Triples):
            if isinstance(o.subject, URI):
                obj = self._resolve_URI(o.subject)
            else:
                obj = rdflib.BNode(o.subject.value)
        elif isinstance(o, BNode):
            obj = rdflib.BNode(o.value)
        else:
            obj = rdflib.Literal(o.value, o.lang, o.type)
        return sub, pred, obj
