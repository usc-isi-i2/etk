import re
from rdflib.namespace import OWL, Namespace, NamespaceManager
from rdflib import URIRef

SCHEMA = Namespace('http://schema.org/')
DIG = Namespace('http://dig.isi.edu/ontologies/dig/')

URI_PATTERN = re.compile(r'^http\:|^urn\:|^info\:|^ftp\:|^https\:')
URI_ABBR_PATTERN = re.compile(r'^([^:]+):([^:]+)$')


class WrongFormatURIException(Exception):
    pass


class PrefixNotFoundException(Exception):
    pass


class OntologyNamespaceManager(NamespaceManager):
    def __init__(self, *args, **kwargs):
        super(OntologyNamespaceManager, self).__init__(*args, **kwargs)
        self.bind('owl', OWL)
        self.bind('schema', SCHEMA)
        self.bind('dig', DIG)

    def parse_uri(self, text):
        """
        Parse input text into URI
        text can be:
          1. URI, directly return
          2. prefix:name, query namespace for prefix, return expanded URI
          3. name, use default namespace to expand it and return it
        """
        if isinstance(text, URIRef):
            return text
        elif isinstance(text, str):
            text = text.strip()
            if URI_PATTERN.match(text):
                return URIRef(text)
            else:
                m = URI_ABBR_PATTERN.match(text)
                if m:
                    prefix, name = m.group()
                    base = self.store.namespace(prefix)
                    if not base:
                        raise PrefixNotFoundException()
                    return URIRef(base + name)
        raise WrongFormatURIException()


