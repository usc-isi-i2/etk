import re
from typing import Optional
from rdflib.namespace import OWL, Namespace, NamespaceManager
from rdflib import URIRef


SCHEMA = Namespace('http://schema.org/')
DIG = Namespace('http://dig.isi.edu/ontologies/dig/')

URI_PATTERN = re.compile(r'^http:|^urn:|^info:|^ftp:|^https:')
URI_ABBR_PATTERN = re.compile(r'^(?:([^:]*):)?([^:]+)$')


class WrongFormatURIException(Exception):
    pass


class PrefixNotFoundException(Exception):
    pass


class PrefixAlreadyUsedException(Exception):
    pass


class OntologyNamespaceManager(NamespaceManager):
    def __init__(self, *args, **kwargs):
        super(OntologyNamespaceManager, self).__init__(*args, **kwargs)
        self.graph.namespace_manager = self
        self.bind('owl', OWL)
        self.bind('schema', SCHEMA)
        self.bind('dig', DIG)

    def parse_uri(self, text: str) -> URIRef:
        """
        Parse input text into URI

        :param text: can be one of
              1. URI, directly return
              2. prefix:name, query namespace for prefix, return expanded URI
              3. name, use default namespace to expand it and return it
        :return: URIRef
        """
        if self.check_uriref(text):
            return self.check_uriref(text)
        elif isinstance(text, str):
            text = text.strip()
            m = URI_ABBR_PATTERN.match(text)
            if m:
                prefix, name = m.groups()
                base = self.store.namespace(prefix if prefix else '')
                if not base:
                    raise PrefixNotFoundException("Prefix: %s", prefix)
                return URIRef(base + name)
        raise WrongFormatURIException()

    def bind(self, prefix: str, namespace: str, override=True, replace=False):
        """
        bind a given namespace to the prefix, forbids same prefix with different namespace

        :param prefix:
        :param namespace:
        :param override: if override, rebind, even if the given namespace is already bound to another prefix.
        :param replace: if replace, replace any existing prefix with the new namespace
        """
        namespace = URIRef(str(namespace))
        # When documenting explain that override only applies in what cases
        if prefix is None:
            prefix = ''
        bound_namespace = self.store.namespace(prefix)
        # Check if the bound_namespace contains a URI and if so convert it into a URIRef for
        # comparison. This is to prevent duplicate namespaces with the same URI.
        if bound_namespace:
            bound_namespace = URIRef(bound_namespace)
        if bound_namespace and bound_namespace != namespace:

            if replace:
                self.store.bind(prefix, namespace)
            # prefix already in use for different namespace
            raise PrefixAlreadyUsedException("Prefix (%s, %s) already used, instead of (%s, %s).",
                                             prefix, self.store.namespace(prefix).toPython(),
                                             prefix, namespace.toPython())
        else:
            bound_prefix = self.store.prefix(namespace)
            if bound_prefix is None:
                self.store.bind(prefix, namespace)
            elif bound_prefix == prefix:
                pass  # already bound
            else:
                if override or bound_prefix.startswith("_"):
                    self.store.bind(prefix, namespace)

    @staticmethod
    def check_uriref(text: str) -> Optional[URIRef]:
        if isinstance(text, URIRef):
            return text
        if isinstance(text, str):
            text = text.strip()
            if URI_PATTERN.match(text.strip()):
                return URIRef(text)
