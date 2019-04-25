from etk.knowledge_graph.node import URI
from etk.knowledge_graph.subject import Reification
import uuid


class Statement(Reification):
    def __init__(self, p1, p2, statement=None):
        if not statement:
            statement_id = str(uuid.uuid4())
            statement = URI('wds:' + '' + statement_id)
        super().__init__(p1, p2, statement)

    def add_qualifier(self, p, value):
        statement_uri = URI('wdv:'+str(uuid.uuid4()))
        reification = Reification(URI('pqv:'+p), value.predicate, statement_uri)
        statement = self.add_property(URI('pq:'+p), value.value, reification)
        self.add_property(URI('rdf:type'), value.type)
        for p, v in value.properties:
            statement.add_property(p, v)

    def add_reference(self, ref):
        self.add_property(URI('prov:wasDerivedFrom'), ref)
